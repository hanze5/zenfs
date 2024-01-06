// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

//推测
class ZonedBlockDevice;         //用于处理分区块设备的类。
class ZonedBlockDeviceBackend;  //用于处理分区块设备后端的类。
class ZoneSnapshot;             //用于处理区域快照的类。
class ZenFSSnapshotOptions;     //用于处理 ZenFS 快照选项的类。

//我也不知道是个什么东西反正包含了 void *data_ 以及 int zone_count_
class ZoneList {
 private:
  void *data_;
  unsigned int zone_count_;

 public:
  ZoneList(void *data, unsigned int zone_count)
      : data_(data), zone_count_(zone_count){};
  void *GetData() { return data_; };
  unsigned int ZoneCount() { return zone_count_; };
  ~ZoneList() { free(data_); };
};

//应用这边感知到的zone
class Zone {
  //
  ZonedBlockDevice *zbd_;
  ZonedBlockDeviceBackend *zbd_be_;
  std::atomic_bool busy_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
                std::unique_ptr<ZoneList> &zones, unsigned int idx);

  uint64_t start_;      //起始地址
  uint64_t capacity_;   /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;          //写指针
  Env::WriteLifeTimeHint lifetime_;      //根据生命周期将相近的放到一起
  std::atomic<uint64_t> used_capacity_;  //已经用的

  IOStatus Reset();     //reset这个zone
  IOStatus Finish();    //
  IOStatus Close();     //

  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();        //判断是否在使用
  bool IsFull();        //判断是否已满
  bool IsEmpty();       //判断是否为空
  uint64_t GetZoneNr(); //用起始地址除以zone_size 可以得知zone逻辑号
  uint64_t GetCapacityLeft();
  bool IsBusy() { return this->busy_.load(std::memory_order_relaxed); }
  bool Acquire() {
    bool expected = false;
    return this->busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }
  bool Release() {
    bool expected = true;
    return this->busy_.compare_exchange_strong(expected, false,
                                               std::memory_order_acq_rel);
  }

  void EncodeJson(std::ostream &json_stream);

  inline IOStatus CheckRelease();
};

//zoned 块存储后端   这个类到时候可能要对接nvme块设备我猜的？？
class ZonedBlockDeviceBackend {
 public:
  uint32_t block_sz_ = 0;   //块大小   open设备的时候就能获取到
  uint64_t zone_sz_ = 0;    //zone大小 open设备的时候就能获取到
  uint32_t nr_zones_ = 0;   //zone数量 open设备的时候就能获取到

 public:
  virtual IOStatus Open(bool readonly, bool exclusive,//指打开这个设备
                        unsigned int *max_active_zones,
                        unsigned int *max_open_zones) = 0;

  virtual std::unique_ptr<ZoneList> ListZones() = 0;
  virtual IOStatus Reset(uint64_t start, bool *offline,
                         uint64_t *max_capacity) = 0;
  virtual IOStatus Finish(uint64_t start) = 0;
  virtual IOStatus Close(uint64_t start) = 0;
  virtual int Read(char *buf, int size, uint64_t pos, bool direct) = 0;
  virtual int Write(char *data, uint32_t size, uint64_t pos) = 0;
  virtual int InvalidateCache(uint64_t pos, uint64_t size) = 0; //用于使缓存失效
  virtual bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones,     //判断zone是否是顺序写zone类型
                         unsigned int idx) = 0;
  virtual bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones, //判断zone是否离线
                             unsigned int idx) = 0;
  virtual bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,//判断zone是否可写
                              unsigned int idx) = 0;
  virtual bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,  //判断zone是否是active的
                            unsigned int idx) = 0;
  virtual bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones,    //判断zone是否是打开的
                          unsigned int idx) = 0;
  virtual uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones, //
                             unsigned int idx) = 0;
  virtual uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                   unsigned int idx) = 0;
  virtual uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual std::string GetFilename() = 0;
  uint32_t GetBlockSize() { return block_sz_; };     //
  uint64_t GetZoneSize() { return zone_sz_; };       //
  uint32_t GetNrZones() { return nr_zones_; };       //
  virtual ~ZonedBlockDeviceBackend(){};
};

enum class ZbdBackendType {
  kBlockDev,
  kZoneFS,
};

class ZonedBlockDevice {
 private:
  std::unique_ptr<ZonedBlockDeviceBackend> zbd_be_;
  std::vector<Zone *> io_zones;          //用于存放数据zone
  std::vector<Zone *> meta_zones;        //用于存放元数据zone
  time_t start_time_;                    //开始使用的时间
  std::shared_ptr<Logger> logger_;       //用于搞日志的？
  uint32_t finish_threshold_ = 0;        //
  std::atomic<uint64_t> bytes_written_{0};  //写入的字节数
  std::atomic<uint64_t> gc_bytes_written_{0};//垃圾回收写入的字节数？

  std::atomic<long> active_io_zones_;    //活动的io zone
  std::atomic<long> open_io_zones_;      //打开的io zone
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;           //zone_resources锁
  std::condition_variable zone_resources_;  //zone_resources条件变量
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;       //IOStatus类型的变量，可能用于存储异步I/O操作的状态。

  std::condition_variable migrate_resource_;
  std::mutex migrate_zone_mtx_;          //可能被用于保护与数据迁移相关的共享资源，确保在进行数据迁移时，相关的资源不会被多个线程同时访问或修改。
  std::atomic<bool> migrating_{false};   //判断是否在进行数据迁移？

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  std::shared_ptr<ZenFSMetrics> metrics_;

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  explicit ZonedBlockDevice(std::string path, ZbdBackendType backend,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly, bool exclusive);  //打开设备

  Zone *GetIOZone(uint64_t offset);              //根据offset获取 Class Zone指针

  IOStatus AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType io_type,
                          Zone **out_zone);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);//这个好像是随便分的 只要前几个zone没有被使用就行

  uint64_t GetFreeSpace();                  //遍历所有zone获取剩余容量
  uint64_t GetUsedSpace();                  //遍历所有zone获取已使用容量
  uint64_t GetReclaimableSpace();           //计算并返回所有已满区域中未使用的空间总量

  std::string GetFilename();                //返回nvme0n1
  uint32_t GetBlockSize();

  IOStatus ResetUnusedIOZones();             //reset所有未使用的数据zone
  void LogZoneStats();                       //调用Info方法，将统计信息记录到日志中。这些信息包括：从开始到现在的时间、已使用的容量、可回收的容量、平均可回收的百分比、活动的区域数量、活动的IO区域数量和打开的IO区域数量
  void LogZoneUsage();                       //调用Info方法，将每个zone的使用量记录到日志
  void LogGarbageInfo();                     //调用Info方法，将全局的zone垃圾情况进行记录

  uint64_t GetZoneSize();                    //获取zone size
  uint32_t GetNrZones();                     //获取zone数量
  std::vector<Zone *> GetMetaZones() { return meta_zones; } //获取包含元数据的zone

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void PutOpenIOZoneToken();
  void PutActiveIOZoneToken();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  int Read(char *buf, uint64_t offset, int n, bool direct);     //设备读
  IOStatus InvalidateCache(uint64_t pos, uint64_t size);

  IOStatus ReleaseMigrateZone(Zone *zone);

  IOStatus TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint lifetime,
                           uint32_t min_capacity);//在需要迁移区域时，获取最佳的开放区域进行迁移

  void AddBytesWritten(uint64_t written) { bytes_written_ += written; };
  void AddGCBytesWritten(uint64_t written) { gc_bytes_written_ += written; };
  uint64_t GetUserBytesWritten() {
    return bytes_written_.load() - gc_bytes_written_.load();
  };
  uint64_t GetTotalBytesWritten() { return bytes_written_.load(); };

 private:
  IOStatus GetZoneDeferredStatus();  
  bool GetActiveIOZoneTokenIfAvailable();        //获取IOZone 活动令牌
  void WaitForOpenIOZoneToken(bool prioritized); //获取IOZone 打开令牌
  IOStatus ApplyFinishThreshold();               //结束剩余容量低于设定阈值且既不空也不满的IOZone
  IOStatus FinishCheapestIOZone();               //找到并结束剩余容量最小的区域
  IOStatus GetBestOpenZoneMatch(Env::WriteLifeTimeHint file_lifetime,//最合适的就是已被使用了但是不满且剩余容量满足输入参数要求
                                unsigned int *best_diff_out, Zone **zone_out,
                                uint32_t min_capacity = 0);
  IOStatus AllocateEmptyZone(Zone **zone_out);   //就从头开始找分配第一个空的
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
