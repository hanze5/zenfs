// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <list>
#include <map>
#include <string>

#include "rocksdb/io_status.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

//存放文件描述符 管理文件描述符 然后就没啥了 就封装了个文件描述符
class ZoneFsFile {
 private:
  int fd_;     //存储文件描述符

 public:
  ZoneFsFile(int fd) : fd_(fd) {}

  ~ZoneFsFile() { close(fd_); }

  int GetFd() { return fd_; }  //获取文件描述符
};

//数据缓存。。
class ZoneFsFileCache {
 private:
  std::list<std::pair<uint64_t, std::shared_ptr<ZoneFsFile>>> list_; //列表 元素是：键值对<zone idx,指向ZoneFsFile的共享指针>
  std::unordered_map<
      uint64_t,
      std::list<std::pair<uint64_t, std::shared_ptr<ZoneFsFile>>>::iterator>
      map_;  //map 元素是<zone idx，pair<zone idx，指向ZoneFsFile的共享指针>>
  std::mutex mtx_;
  unsigned max_;
  int flags_;

 public:
  explicit ZoneFsFileCache(int flags);  
  ~ZoneFsFileCache();

  void Put(uint64_t zone);     //清楚掉缓存里的zone idx所包含的shared_ptr<ZoneFsFile>
  std::shared_ptr<ZoneFsFile> Get(uint64_t zone, std::string filename);//通过zone idx 找到她所包含的名为filename的hared_ptr<ZoneFsFile>
  void Resize(unsigned new_size);//其实就是调用Prune去更改容量

 private:
  void Prune(unsigned limit);  //设定map 与list的数量限制 如果超过就会抹掉后面几个
};

class ZoneFsBackend : public ZonedBlockDeviceBackend {
 private:
  std::string mountpoint_;   //挂载点
  int zone_zero_fd_;         //
  bool readonly_;
  ZoneFsFileCache rd_fds_;
  ZoneFsFileCache direct_rd_fds_;
  ZoneFsFileCache wr_fds_;

 public:
  explicit ZoneFsBackend(std::string mountpoint);//填充一下成员变量
  ~ZoneFsBackend();

  IOStatus Open(bool readonly, bool exclusive, unsigned int *max_active_zones,
                unsigned int *max_open_zones);//打开一个Zone文件系统，并获取一些重要的参数，如区域的数量、区域的大小、块的大小、最大活动区域和最大开放区域等
  std::unique_ptr<ZoneList> ListZones();      //也是创建了挂载点的ZoneList
  IOStatus Reset(uint64_t start, bool *offline, uint64_t *max_capacity);//重置指定的区域，并没有直接执行Zone的reset操作。这个Reset方法主要是将指定的区域文件大小设置为0（通过truncate函数），并更新区域的状态和最大容量
  IOStatus Finish(uint64_t start);//目的是完成一个特定的区域
  IOStatus Close(uint64_t start); //调用 PutZoneFile(start, O_WRONLY) 函数，将文件放回文件池。这里的 start 参数表示区域的起始位置
  int Read(char *buf, int size, uint64_t pos, bool direct);//本质上还是调用pread
  int Write(char *data, uint32_t size, uint64_t pos);      //本质上是调用pwrite
  int InvalidateCache(uint64_t pos, uint64_t size);//调用 posix_fadvise(file->GetFd(), offset, size, POSIX_FADV_DONTNEED) 函数，使得从偏移量 offset 开始，长度为 size 的文件区域的缓存失效。这里的 POSIX_FADV_DONTNEED 是一个建议，表示操作系统不再需要这部分的缓存数据。

  bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones, unsigned int idx);         //zone是否是顺序写
  bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones, unsigned int idx);     //zone是否是离线的
  bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones, unsigned int idx);    //zone是否可写入
  bool ZoneIsActive(std::unique_ptr<ZoneList> &zones, unsigned int idx);      //zone是否是Active的
  bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones2, unsigned int idx);       //zone是否是Open的
  uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones, unsigned int idx);     //zone的起始地址
  uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones, unsigned int idx);//zone的最大容量
  uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones, unsigned int idx);        //zone的写指针
  std::string GetFilename() { return mountpoint_; }                           //获取文件名 也就是挂载点

 private:
  std::string ErrorToString(int err);  
  uint64_t LBAToZoneOffset(uint64_t pos);   //通过pos可以计算出在zone内的偏移量
  std::string LBAToZoneFile(uint64_t start);//可以通过起始地址 返回<mountpoint>/seq/<zone idx>
  std::string GetBackingDevice(const char *mountpoint);//根据挂载点可以获得挂载设备名 如：nvme0n1
  unsigned int GetSysFsValue(std::string dev_name, std::string field);//读取/sys/fs/zonefs/目录下指定设备的指定字段的值
  std::shared_ptr<ZoneFsFile> GetZoneFile(uint64_t start, int flags);//根据输入的start和flags参数，从相应的文件描述符缓存中获取指定的区域文件
  void PutZoneFile(uint64_t start, int flags);//在完成对一个区域的写操作后，将该区域从写文件描述符缓存中移除，以便释放资源
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
