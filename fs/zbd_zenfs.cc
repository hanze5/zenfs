// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "snapshot.h"
#include "zbdlib_zenfs.h"
#include "zonefs_zenfs.h"

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 * dz modified
 */
#define ZENFS_META_ZONES (3)

#define WORKLOADS_NUM (4)

/* Minimum of number of zones that makes sense dz added 保证每个工作负载都有32个*/
#define ZENFS_MIN_ZONES (32*4)

namespace ROCKSDB_NAMESPACE {

//zone的构造函数 需传入设备  设备后端   以及 
Zone::Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
           std::unique_ptr<ZoneList> &zones, unsigned int idx)
    : zbd_(zbd),
      zbd_be_(zbd_be),
      busy_(false),
      start_(zbd_be->ZoneStart(zones, idx)),
      max_capacity_(zbd_be->ZoneMaxCapacity(zones, idx)),
      wp_(zbd_be->ZoneWp(zones, idx)) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (zbd_be->ZoneIsWritable(zones, idx))
    capacity_ = max_capacity_ - (wp_ - start_);
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

//zone信息json格式
void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}

IOStatus Zone::Reset() {
  bool offline;
  uint64_t max_capacity;

  assert(!IsUsed());
  assert(IsBusy());

  IOStatus ios = zbd_be_->Reset(start_, &offline, &max_capacity);
  if (ios != IOStatus::OK()) return ios;

  if (offline)
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = max_capacity;

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  assert(IsBusy());

  IOStatus ios = zbd_be_->Finish(start_);
  if (ios != IOStatus::OK()) return ios;

  capacity_ = 0;
  wp_ = start_ + zbd_->GetZoneSize();

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  assert(IsBusy());

  if (!(IsEmpty() || IsFull())) {
    IOStatus ios = zbd_be_->Close(start_);
    if (ios != IOStatus::OK()) return ios;
  }

  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size) {
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_ZONE_WRITE_LATENCY,
                                 Env::Default());
  zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_WRITE_THROUGHPUT, size);
  char *ptr = data;
  uint32_t left = size;
  int ret;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    ret = zbd_be_->Write(ptr, left, wp_);
    if (ret < 0) {
      return IOStatus::IOError(strerror(errno));
    }

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
    zbd_->AddBytesWritten(ret);
  }

  return IOStatus::OK();
}

inline IOStatus Zone::CheckRelease() {
  if (!Release()) {
    assert(false);
    return IOStatus::Corruption("Failed to unset busy flag of zone " +
                                std::to_string(GetZoneNr()));
  }

  return IOStatus::OK();
}
//根据 offset返回Class Zone的指针
Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zbd_be_->GetZoneSize()))
      return z;
  return nullptr;
}

//ZonedBlockDevice的构造函数  最主要的就是根据ZbdBackendType类型 初始化ZbdlibBackend(path) 
ZonedBlockDevice::ZonedBlockDevice(std::string path, ZbdBackendType backend,
                                   std::shared_ptr<Logger> logger,
                                   std::shared_ptr<ZenFSMetrics> metrics)
    : logger_(logger), metrics_(metrics) {
  if (backend == ZbdBackendType::kBlockDev) {
    zbd_be_ = std::unique_ptr<ZbdlibBackend>(new ZbdlibBackend(path));
    Info(logger_, "New Zoned Block Device: %s", zbd_be_->GetFilename().c_str());
  } else if (backend == ZbdBackendType::kZoneFS) {
    zbd_be_ = std::unique_ptr<ZoneFsBackend>(new ZoneFsBackend(path));
    Info(logger_, "New zonefs backing: %s", zbd_be_->GetFilename().c_str());
  }
}

IOStatus ZonedBlockDevice::Open(bool readonly, bool exclusive) {
  std::unique_ptr<ZoneList> zone_rep; //说明同时只能有一个ZoneList指针zone_rep
  unsigned int max_nr_active_zones;   //最大 active zone数
  unsigned int max_nr_open_zones;     //最大 open zone数
  Status s;                           //rocksdb里面表示状态的类
  uint64_t i = 0;                     
  uint64_t m;
  // Reserve one zone for metadata and another one for extent migration
  /**
   * 它提到了为元数据和迁移扩展分别保留一个区域。
   * 元数据是描述其他数据的数据，它提供了有关数据的详细信息，如其格式或者创建日期等。
   * 迁移扩展则可能是指在数据迁移过程中需要的额外存储空间。
   * 这两个区域的保留可以确保在处理文件系统数据时，元数据和迁移扩展的处理不会受到影响，从而提高文件系统的稳定性和效率。
  */
  int reserved_zones = 2;

  //写操作打开zone必须是独占
  if (!readonly && !exclusive)
    return IOStatus::InvalidArgument("Write opens must be exclusive");

  IOStatus ios = zbd_be_->Open(readonly, exclusive, &max_nr_active_zones,
                               &max_nr_open_zones);

  if (ios != IOStatus::OK()) return ios;
  //确定zbd设备zone数不小于32个 这个是zenfs定的
  if (zbd_be_->GetNrZones() < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported("To few zones on zoned backend (" +
                                  std::to_string(ZENFS_MIN_ZONES) +
                                  " required)");
  }

  //获取max_nr_active_zones max_nr_open_zones
  if (max_nr_active_zones == 0)
    max_nr_active_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_active_io_zones_ = max_nr_active_zones - reserved_zones;

  if (max_nr_open_zones == 0)
    max_nr_open_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_open_io_zones_ = max_nr_open_zones - reserved_zones;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       zbd_be_->GetNrZones(), max_nr_active_zones, max_nr_open_zones);

  zone_rep = zbd_be_->ListZones();
  if (zone_rep == nullptr || zone_rep->ZoneCount() != zbd_be_->GetNrZones()) {
    Error(logger_, "Failed to list zones");
    return IOStatus::IOError("Failed to list zones");
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for(uint64_t j = 0;j<WORKLOADS_NUM;j++)
  {
    m = 0;
    //就前三个zone当元数据zone
    while (m < ZENFS_META_ZONES && i < (j+1)*(zone_rep->ZoneCount()/WORKLOADS_NUM)) {
      /* Only use sequential write required zones */
      if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
        if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
          //
          meta_zones.push_back(new Zone(this, zbd_be_.get(), zone_rep, i));
        }
        m++;
      }
      i++;
    }
    for (; i <(j+1)*(zone_rep->ZoneCount()/WORKLOADS_NUM); i++) {
      /* Only use sequential write required zones */
      if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
        if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
          Zone *newZone = new Zone(this, zbd_be_.get(), zone_rep, i);
          if (!newZone->Acquire()) {
            assert(false);
            return IOStatus::Corruption("Failed to set busy flag of zone " +
                                        std::to_string(newZone->GetZoneNr()));
          }
          //
          io_zones.push_back(newZone);
          if (zbd_be_->ZoneIsActive(zone_rep, i)) {
            active_io_zones_++;
            if (zbd_be_->ZoneIsOpen(zone_rep, i)) {
              if (!readonly) {
                newZone->Close();
              }
            }
          }
          IOStatus status = newZone->CheckRelease();
          if (!status.ok()) {
            return status;
          }
        }
      }
    }
  }

  start_time_ = time(NULL);

  return IOStatus::OK();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());
}

//
void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

void ZonedBlockDevice::LogGarbageInfo() {
  // Log zone garbage stats vector.
  //
  // The values in the vector represents how many zones with target garbage
  // percent. Garbage percent of each index: [0%, <10%, < 20%, ... <100%, 100%]
  // For example `[100, 1, 2, 3....]` means 100 zones are empty, 1 zone has less
  // than 10% garbage, 2 zones have  10% ~ 20% garbage ect.
  //
  // We don't need to lock io_zones since we only read data and we don't need
  // the result to be precise.
  //这行代码初始化了一个名为zone_gc_stat的数组，用于存储各个垃圾百分比对应的区域数量
  int zone_gc_stat[12] = {0};
  for (auto z : io_zones) {
    if (!z->Acquire()) {
      continue;
    }

    if (z->IsEmpty()) {
      zone_gc_stat[0]++;
      z->Release();
      continue;
    }

    double garbage_rate = 0;
    if (z->IsFull()) {
      garbage_rate =
          double(z->max_capacity_ - z->used_capacity_) / z->max_capacity_;
    } else {
      garbage_rate =
          double(z->wp_ - z->start_ - z->used_capacity_) / z->max_capacity_;
    }
    assert(garbage_rate >= 0);
    int idx = int((garbage_rate + 0.1) * 10);
    zone_gc_stat[idx]++;

    z->Release();
  }

  std::stringstream ss;
  ss << "Zone Garbage Stats: [";
  for (int i = 0; i < 12; i++) {
    ss << zone_gc_stat[i] << " ";
  }
  ss << "]";
  Info(logger_, "%s", ss.str().data());
}


ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_COULD_BE_WORSE (50)
//计算区域的生命周期和文件的生命周期之间的差异，以便于进行生命周期管理。
unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;
  if (zone_lifetime == file_lifetime) return LIFETIME_DIFF_COULD_BE_WORSE;

  return LIFETIME_DIFF_NOT_GOOD;
}

IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) { 
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}
/**
 * dz added：
 * 提取db名称
*/
std::string ExtractSecondToLastDirName(const std::string& file_path) {
    size_t last_slash_pos = file_path.find_last_of('/');
    size_t second_last_slash_pos = file_path.find_last_of('/', last_slash_pos - 1);
    if (last_slash_pos != std::string::npos && second_last_slash_pos != std::string::npos) {
        return file_path.substr(second_last_slash_pos + 1, last_slash_pos - second_last_slash_pos - 1);
    }
    return ""; // 如果路径中没有足够的 '/'，返回空字符串
}
/**
 * dz added:
 * AllocateMetaZone 重载  源文件也要分开放 每个工作负载两个zone源文件zone
*/
IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone,std::string fname) { 
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}

IOStatus ZonedBlockDevice::ResetUnusedIOZones() {
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed()) {
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        IOStatus release_status = z->CheckRelease();
        if (!reset_status.ok()) return reset_status;
        if (!release_status.ok()) return release_status;
        if (!full) PutActiveIOZoneToken();
      } else {
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
      }
    }
  }
  return IOStatus::OK();
}

void ZonedBlockDevice::WaitForOpenIOZoneToken(bool prioritized) {
  long allocator_open_limit;

  /* Avoid non-priortized allocators from starving prioritized ones */
  if (prioritized) {
    allocator_open_limit = max_nr_open_io_zones_;
  } else {
    allocator_open_limit = max_nr_open_io_zones_ - 1;
  }

  /* Wait for an open IO Zone token - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutOpenIOZoneToken to return the resource
   * 这段代码是为了防止非优先级的分配器饿死优先级的分配器。
   * 如果prioritized为真，那么分配器的打开限制就是max_nr_open_io_zones_；
   * 否则，分配器的打开限制就是max_nr_open_io_zones_ - 1。
   * 这样做的目的是确保优先级的分配器总是能够获得足够的资源，避免因为资源不足而无法进行操作
   */
  
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  zone_resources_.wait(lk, [this, allocator_open_limit] {
    if (open_io_zones_.load() < allocator_open_limit) {
      open_io_zones_++;
      return true;
    } else {
      return false;
    }
  });
}

bool ZonedBlockDevice::GetActiveIOZoneTokenIfAvailable() {
  /* Grap an active IO Zone token if available - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutActiveIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  if (active_io_zones_.load() < max_nr_active_io_zones_) {
    active_io_zones_++;
    return true;
  }
  return false;
}
//释放iozone 打开令牌
void ZonedBlockDevice::PutOpenIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    open_io_zones_--;
  }
  zone_resources_.notify_one();
}
//释放iozone 活动令牌
void ZonedBlockDevice::PutActiveIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    active_io_zones_--;
  }
  zone_resources_.notify_one();
}
/**
 * 检查设备中的每个 IOZone，看它们的剩余容量是否低于设定的阈值（finish_threshold_）。
 * 如果一个区域的剩余容量低于这个阈值，并且该区域既不为空也不满，
 * 则会调用 Finish 方法来完成该区域
*/
IOStatus ZonedBlockDevice::ApplyFinishThreshold() {
  IOStatus s;

  if (finish_threshold_ == 0) return IOStatus::OK();

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      //检查容量是否小于总容量一定的百分比阈值
      bool within_finish_threshold =
          z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100);

      //如果区域既不为空也不满，并且满足上述条件
      if (!(z->IsEmpty() || z->IsFull()) && within_finish_threshold) {
        /* If there is less than finish_threshold_% remaining capacity in a
         * non-open-zone, finish the zone */
        s = z->Finish();
        if (!s.ok()) {
          z->Release();
          Debug(logger_, "Failed finishing zone");
          return s;
        }
        s = z->CheckRelease();
        if (!s.ok()) return s;
        //finish了就是不活动了 释放iozone活动令牌
        PutActiveIOZoneToken();
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::FinishCheapestIOZone() {
  IOStatus s;
  Zone *finish_victim = nullptr;

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty() || z->IsFull()) {
        s = z->CheckRelease();
        if (!s.ok()) return s;
        continue;
      }
      if (finish_victim == nullptr) {
        finish_victim = z;
        continue;
      }
      if (finish_victim->capacity_ > z->capacity_) {
        s = finish_victim->CheckRelease();
        if (!s.ok()) return s;
        finish_victim = z;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  // If all non-busy zones are empty or full, we should return success.
  if (finish_victim == nullptr) {
    Info(logger_, "All non-busy zones are empty or full, skip.");
    return IOStatus::OK();
  }

  s = finish_victim->Finish();
  IOStatus release_status = finish_victim->CheckRelease();

  if (s.ok()) {
    PutActiveIOZoneToken();
  }
  if (!release_status.ok()) {
    return release_status;
  }
  return s;
}

/**
 * dz added:
 * FinishCheapestIOZone 重载 会按照fname进行关闭 保证不会关闭其他应用的
*/
IOStatus ZonedBlockDevice::FinishCheapestIOZone(std:: string db_name) {
  IOStatus s;
  Zone *finish_victim = nullptr;

  //每4个zone 各分给4个app 1个
  int group = -1;
  if(db_name=="1st"){
    group=0;
  }else if(db_name=="2nd"){
    group=1;
  }else if(db_name=="3rd"){
    group=2;
  }else if(db_name=="4th"){
    group=3;
  }else{
    std::cout<<"dz ZonedBlockDevice::FinishCheapestIOZone:db_name is "+db_name<<std::endl;
  }

  if(group==-1){
    for (const auto z : io_zones) {
      if (z->Acquire()) {
        if (z->IsEmpty() || z->IsFull()) {
          s = z->CheckRelease();
          if (!s.ok()) return s;
          continue;
        }
        if (finish_victim == nullptr) {
          finish_victim = z;
          continue;
        }
        if (finish_victim->capacity_ > z->capacity_) {
          s = finish_victim->CheckRelease();
          if (!s.ok()) return s;
          finish_victim = z;
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      }
    }
  }else{
    unsigned int zone_per_group = io_zones.size()/4;
    size_t start = zone_per_group*group;
    size_t end =  zone_per_group*(group+1);
    end = end<io_zones.size()?end:io_zones.size();
    for(size_t i = start;i<end;i++){
      Zone * z = io_zones[i];
      if (z->Acquire()) {
        if (z->IsEmpty() || z->IsFull()) {
          s = z->CheckRelease();
          if (!s.ok()) return s;
          continue;
        }
        if (finish_victim == nullptr) {
          finish_victim = z;
          continue;
        }
        if (finish_victim->capacity_ > z->capacity_) {
          s = finish_victim->CheckRelease();
          if (!s.ok()) return s;
          finish_victim = z;
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      }
    }
  }
  // If all non-busy zones are empty or full, we should return success.
  if (finish_victim == nullptr) {
    Info(logger_, "All non-busy zones are empty or full, skip.");
    return IOStatus::OK();
  }

  s = finish_victim->Finish();
  IOStatus release_status = finish_victim->CheckRelease();

  if (s.ok()) {
    PutActiveIOZoneToken();
  }
  if (!release_status.ok()) {
    return release_status;
  }
  return s;
}

IOStatus ZonedBlockDevice::GetBestOpenZoneMatch(
    Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out,
    Zone **zone_out, uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if ((z->used_capacity_ > 0) && !z->IsFull() &&
          z->capacity_ >= min_capacity) {
        unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
        if (diff <= best_diff) {
          if (allocated_zone != nullptr) {
            s = allocated_zone->CheckRelease();
            if (!s.ok()) {
              IOStatus s_ = z->CheckRelease();
              if (!s_.ok()) return s_;
              return s;
            }
          }
          allocated_zone = z;
          best_diff = diff;
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  *best_diff_out = best_diff;
  *zone_out = allocated_zone;

  return IOStatus::OK();
}

/**
 * dz added：
 * GetBestOpenZoneMatch函数重载 使之隔离app
*/
IOStatus ZonedBlockDevice::GetBestOpenZoneMatch(
    Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out,
    Zone **zone_out,std::string db_name, uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;
  //每4个zone 各分给4个app 1个
  int group = -1;
  if(db_name=="1st"){
    group=0;
  }else if(db_name=="2nd"){
    group=1;
  }else if(db_name=="3rd"){
    group=2;
  }else if(db_name=="4th"){
    group=3;
  }else{
    std::cout<<"dz ZonedBlockDevice::GetBestOpenZoneMatch :db_name is "+db_name<<std::endl;
  }
  if(group == -1){
    for (const auto z : io_zones) {
      if (z->Acquire()) {
        if ((z->used_capacity_ > 0) && !z->IsFull() &&
            z->capacity_ >= min_capacity) {
          unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
          if (diff <= best_diff) {
            if (allocated_zone != nullptr) {
              s = allocated_zone->CheckRelease();
              if (!s.ok()) {
                IOStatus s_ = z->CheckRelease();
                if (!s_.ok()) return s_;
                return s;
              }
            }
            allocated_zone = z;
            best_diff = diff;
          } else {
            s = z->CheckRelease();
            if (!s.ok()) return s;
          }
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      }
    }
    *best_diff_out = best_diff;
    *zone_out = allocated_zone;
  }else{
    unsigned int zone_per_group = io_zones.size()/4;
    size_t start = zone_per_group*group;
    size_t end =  zone_per_group*(group+1);
    end = end<io_zones.size()?end:io_zones.size();
    for(size_t i = start;i<end;i++){
      Zone * z = io_zones[i];
      if (z->Acquire()) {
        if ((z->used_capacity_ > 0) && !z->IsFull() &&
            z->capacity_ >= min_capacity) {
          unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
          if (diff <= best_diff) {
            if (allocated_zone != nullptr) {
              s = allocated_zone->CheckRelease();
              if (!s.ok()) {
                IOStatus s_ = z->CheckRelease();
                if (!s_.ok()) return s_;
                return s;
              }
            }
            allocated_zone = z;
            best_diff = diff;
          } else {
            s = z->CheckRelease();
            if (!s.ok()) return s;
          }
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      }
    }
  }

  return IOStatus::OK();
}

// TODO
IOStatus ZonedBlockDevice::AllocateEmptyZone(Zone **zone_out) {
  IOStatus s;
  Zone *allocated_zone = nullptr;
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty()) {
        allocated_zone = z;
        break;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }
  *zone_out = allocated_zone;
  return IOStatus::OK();
}

/**
 * dz added
 * 加入 AllocateEmptyZone函数的重载 根据fname找empty
*/
IOStatus ZonedBlockDevice::AllocateEmptyZone(Zone **zone_out,std::string db_name) {
  IOStatus s;
  Zone *allocated_zone = nullptr;
  //每4个zone 各分给4个app 1个
  int group = -1;
  if(db_name=="1st"){
    group=0;
  }else if(db_name=="2nd"){
    group=1;
  }else if(db_name=="3rd"){
    group=2;
  }else if(db_name=="4th"){
    group=3;
  }else{
    std::cout<<"dz ZonedBlockDevice::AllocateEmptyZone:db_name is "+db_name<<std::endl;
  }

  if(group == -1){
    for (const auto z : io_zones) {
      if (z->Acquire()) {
        if (z->IsEmpty()) {
          allocated_zone = z;
          break;
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      }
    }
  }else{
    unsigned int zone_per_group = io_zones.size()/4;
    size_t start = zone_per_group*group;
    size_t end =  zone_per_group*(group+1);
    end = end<io_zones.size()?end:io_zones.size();
    for(size_t i = start;i<end;i++){
      Zone * z = io_zones[i];
      if (z->Acquire()) {
        if (z->IsEmpty()) {
          allocated_zone = z;
          break;
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      }
    }
  }
  *zone_out = allocated_zone;
  return IOStatus::OK();
}

//使某一部分区域的缓存失效
IOStatus ZonedBlockDevice::InvalidateCache(uint64_t pos, uint64_t size) {
  int ret = zbd_be_->InvalidateCache(pos, size);

  if (ret) {
    return IOStatus::IOError("Failed to invalidate cache");
  }
  return IOStatus::OK();
}

int ZonedBlockDevice::Read(char *buf, uint64_t offset, int n, bool direct) {
  int ret = 0;
  int left = n;
  int r = -1;

  while (left) {
    r = zbd_be_->Read(buf, left, offset, direct);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ret += r;
    buf += r;
    left -= r;
    offset += r;
  }

  if (r < 0) return r;
  return ret;
}

//释放迁移zone？？
IOStatus ZonedBlockDevice::ReleaseMigrateZone(Zone *zone) {
  IOStatus s = IOStatus::OK();
  {
    std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
    migrating_ = false;
    if (zone != nullptr) {
      s = zone->CheckRelease();
      Info(logger_, "ReleaseMigrateZone: %lu", zone->start_);
    }
  }
  migrate_resource_.notify_one();
  return s;
}

IOStatus ZonedBlockDevice::TakeMigrateZone(Zone **out_zone,
                                           Env::WriteLifeTimeHint file_lifetime,
                                           uint32_t min_capacity) {
  std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
  migrate_resource_.wait(lock, [this] { return !migrating_; });

  migrating_ = true;

  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  auto s =
      GetBestOpenZoneMatch(file_lifetime, &best_diff, out_zone, min_capacity);
  if (s.ok() && (*out_zone) != nullptr) {
    Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
  } else {
    migrating_ = false;
  }

  return s;
}

/**
 * dz added:
 * TakeMigrateZone函数重载
*/
IOStatus ZonedBlockDevice::TakeMigrateZone(Zone **out_zone,
                                           Env::WriteLifeTimeHint file_lifetime,
                                           uint32_t min_capacity,std::string db_name) {
  std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
  migrate_resource_.wait(lock, [this] { return !migrating_; });

  migrating_ = true;

  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  auto s =
      GetBestOpenZoneMatch(file_lifetime, &best_diff, out_zone,db_name, min_capacity);
  if (s.ok() && (*out_zone) != nullptr) {
    Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
  } else {
    migrating_ = false;
  }

  return s;
}

IOStatus ZonedBlockDevice::AllocateIOZone(Env::WriteLifeTimeHint file_lifetime,
                                          IOType io_type, Zone **out_zone) {
  //指针将用于存储被分配的IO区域
  Zone *allocated_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;//将用于存储最佳生命周期差值
  int new_zone = 0;
  IOStatus s;

  auto tag = ZENFS_WAL_IO_ALLOC_LATENCY;    //用于标记io分配的延迟类型
  //检查IO类型是否不等于IOType::kWAL。如果不等于，那么根据文件的生命周期，将tag设置为ZENFS_L0_IO_ALLOC_LATENCY或ZENFS_NON_WAL_IO_ALLOC_LATENCY
  if (io_type != IOType::kWAL) {
    // L0 flushes have lifetime MEDIUM
    if (file_lifetime == Env::WLTH_MEDIUM) {
      tag = ZENFS_L0_IO_ALLOC_LATENCY;
    } else {
      tag = ZENFS_NON_WAL_IO_ALLOC_LATENCY;
    }
  }
  //用于监控IO分配的延迟
  ZenFSMetricsLatencyGuard guard(metrics_, tag, Env::Default());
  /**
   * 调用metrics_的ReportQPS方法，报告每秒查询数（QPS）
  */
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  if (io_type != IOType::kWAL) {
    s = ApplyFinishThreshold();
    if (!s.ok()) {
      return s;
    }
  }
  //待一个开放的IO区域令牌。如果io_type等于IOType::kWAL，则优先等待
  WaitForOpenIOZoneToken(io_type == IOType::kWAL);

  /* Try to fill an already open zone(with the best life time diff) */
  /* 尝试找到一个已经打开的区域（具有最佳生命周期差异）*/
  s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone);
  if (!s.ok()) {
    PutOpenIOZoneToken();
    return s;
  }

  // Holding allocated_zone if != nullptr
  //判断当前的生命周期差异是否超过了一个可接受的范围。如果超过了这个范围，那么可能需要进行一些额外的操作，比如选择一个新的IO区域进行写入。
  if (best_diff >= LIFETIME_DIFF_COULD_BE_WORSE) {
    //尝试获取一个可用的活动IO区域令牌，如果获取成功，got_token为真
    bool got_token = GetActiveIOZoneTokenIfAvailable();

    /* If we did not get a token, try to use the best match, even if the life
     * time diff not good but a better choice than to finish an existing zone
     * and open a new one
     * 如果我们没有获取到令牌，我们会尝试使用最佳匹配的区域，即使这个区域的生命周期差异不理想。
     * 这样做比结束一个已经存在的区域并开启一个新的区域要好。
     * 这是因为结束一个已经存在的区域并开启一个新的区域会消耗更多的资源和时间。
     * 因此，即使生命周期差异不理想，使用最佳匹配的区域仍然是一个更好的选择
     */
    if (allocated_zone != nullptr) {
      if (!got_token && best_diff == LIFETIME_DIFF_COULD_BE_WORSE) {
        Debug(logger_,
              "Allocator: avoided a finish by relaxing lifetime diff "
              "requirement\n");
      } else {
        //查分配的IO区域是否可以被释放，并将结果存储在变量s中。
        s = allocated_zone->CheckRelease();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          if (got_token) PutActiveIOZoneToken();
          return s;
        }
        allocated_zone = nullptr;
      }
    }

    /* If we haven't found an open zone to fill, open a new zone */
    if (allocated_zone == nullptr) {
      /* We have to make sure we can open an empty zone */
      /**
       * 只要没有获取到令牌并且没有可用的活动IO区域令牌，就会执行循环体中的代码。
       * 循环体中的代码主要是尝试结束最便宜的IO区域，并在结束失败时返回错误状态
      */
      while (!got_token && !GetActiveIOZoneTokenIfAvailable()) {
        s = FinishCheapestIOZone();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          return s;
        }
      }
      //找一个空的
      s = AllocateEmptyZone(&allocated_zone);
      if (!s.ok()) {
        PutActiveIOZoneToken();
        PutOpenIOZoneToken();
        return s;
      }

      if (allocated_zone != nullptr) {
        assert(allocated_zone->IsBusy());
        //将分配的IO区域的生命周期设置为文件的生命周期
        allocated_zone->lifetime_ = file_lifetime;
        new_zone = true;
      } else {
        PutActiveIOZoneToken();
      }
    }
  }

  if (allocated_zone) {
    assert(allocated_zone->IsBusy());
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  } else {
    PutOpenIOZoneToken();
  }

  if (io_type != IOType::kWAL) {
    LogZoneStats();
  }

  *out_zone = allocated_zone;

  //报告当前开放的IO区域数量和活动的IO区域数量
  metrics_->ReportGeneral(ZENFS_OPEN_ZONES_COUNT, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES_COUNT, active_io_zones_);

  return IOStatus::OK();
}


/**
 * dz added:
 * AllocateIOZone函数重载
*/
IOStatus ZonedBlockDevice::AllocateIOZone(Env::WriteLifeTimeHint file_lifetime,
                                          IOType io_type, Zone **out_zone,std::string fname) {
  //指针将用于存储被分配的IO区域
  Zone *allocated_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;//将用于存储最佳生命周期差值
  int new_zone = 0;
  IOStatus s;

  std::string db_name = ExtractSecondToLastDirName(fname);
  std::cout<<"dz ZonedBlockDevice::AllocateIOZone: db_name is "<<db_name<<std::endl;

  auto tag = ZENFS_WAL_IO_ALLOC_LATENCY;    //用于标记io分配的延迟类型
  //检查IO类型是否不等于IOType::kWAL。如果不等于，那么根据文件的生命周期，将tag设置为ZENFS_L0_IO_ALLOC_LATENCY或ZENFS_NON_WAL_IO_ALLOC_LATENCY
  if (io_type != IOType::kWAL) {
    // L0 flushes have lifetime MEDIUM
    if (file_lifetime == Env::WLTH_MEDIUM) {
      tag = ZENFS_L0_IO_ALLOC_LATENCY;
    } else {
      tag = ZENFS_NON_WAL_IO_ALLOC_LATENCY;
    }
  }
  //用于监控IO分配的延迟
  ZenFSMetricsLatencyGuard guard(metrics_, tag, Env::Default());
  /**
   * 调用metrics_的ReportQPS方法，报告每秒查询数（QPS）
  */
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  if (io_type != IOType::kWAL) {
    s = ApplyFinishThreshold();
    if (!s.ok()) {
      return s;
    }
  }
  //待一个开放的IO区域令牌。如果io_type等于IOType::kWAL，则优先等待
  WaitForOpenIOZoneToken(io_type == IOType::kWAL);

  /* Try to fill an already open zone(with the best life time diff) */
  /* 尝试找到一个已经打开的区域（具有最佳生命周期差异）*/
  s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone,db_name);
  if (!s.ok()) {
    PutOpenIOZoneToken();
    return s;
  }

  // Holding allocated_zone if != nullptr
  //判断当前的生命周期差异是否超过了一个可接受的范围。如果超过了这个范围，那么可能需要进行一些额外的操作，比如选择一个新的IO区域进行写入。
  if (best_diff >= LIFETIME_DIFF_COULD_BE_WORSE) {
    //尝试获取一个可用的活动IO区域令牌，如果获取成功，got_token为真
    bool got_token = GetActiveIOZoneTokenIfAvailable();

    /* If we did not get a token, try to use the best match, even if the life
     * time diff not good but a better choice than to finish an existing zone
     * and open a new one
     * 如果我们没有获取到令牌，我们会尝试使用最佳匹配的区域，即使这个区域的生命周期差异不理想。
     * 这样做比结束一个已经存在的区域并开启一个新的区域要好。
     * 这是因为结束一个已经存在的区域并开启一个新的区域会消耗更多的资源和时间。
     * 因此，即使生命周期差异不理想，使用最佳匹配的区域仍然是一个更好的选择
     */
    if (allocated_zone != nullptr) {
      if (!got_token && best_diff == LIFETIME_DIFF_COULD_BE_WORSE) {
        Debug(logger_,
              "Allocator: avoided a finish by relaxing lifetime diff "
              "requirement\n");
      } else {
        //查分配的IO区域是否可以被释放，并将结果存储在变量s中。
        s = allocated_zone->CheckRelease();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          if (got_token) PutActiveIOZoneToken();
          return s;
        }
        allocated_zone = nullptr;
      }
    }

    /* If we haven't found an open zone to fill, open a new zone */
    if (allocated_zone == nullptr) {
      /* We have to make sure we can open an empty zone */
      /**
       * 只要没有获取到令牌并且没有可用的活动IO区域令牌，就会执行循环体中的代码。
       * 循环体中的代码主要是尝试结束最便宜的IO区域，并在结束失败时返回错误状态
      */
      while (!got_token && !GetActiveIOZoneTokenIfAvailable()) {
        s = FinishCheapestIOZone(db_name);
        if (!s.ok()) {
          PutOpenIOZoneToken();
          return s;
        }
      }
      //找一个空的
      s = AllocateEmptyZone(&allocated_zone,db_name);
      if (!s.ok()) {
        PutActiveIOZoneToken();
        PutOpenIOZoneToken();
        return s;
      }

      if (allocated_zone != nullptr) {
        assert(allocated_zone->IsBusy());
        //将分配的IO区域的生命周期设置为文件的生命周期
        allocated_zone->lifetime_ = file_lifetime;
        new_zone = true;
      } else {
        PutActiveIOZoneToken();
      }
    }
  }

  if (allocated_zone) {
    assert(allocated_zone->IsBusy());
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  } else {
    PutOpenIOZoneToken();
  }

  if (io_type != IOType::kWAL) {
    LogZoneStats();
  }

  *out_zone = allocated_zone;

  //报告当前开放的IO区域数量和活动的IO区域数量
  metrics_->ReportGeneral(ZENFS_OPEN_ZONES_COUNT, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES_COUNT, active_io_zones_);

  return IOStatus::OK();
}

std::string ZonedBlockDevice::GetFilename() { return zbd_be_->GetFilename(); }

uint32_t ZonedBlockDevice::GetBlockSize() { return zbd_be_->GetBlockSize(); }

uint64_t ZonedBlockDevice::GetZoneSize() { return zbd_be_->GetZoneSize(); }

uint32_t ZonedBlockDevice::GetNrZones() { return zbd_be_->GetNrZones(); }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

IOStatus ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(IOStatus status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto *zone : io_zones) {
    snapshot.emplace_back(*zone);
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
