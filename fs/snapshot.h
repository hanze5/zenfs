// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "io_zenfs.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

// Indicate what stats info we want. 显示了我们需要的信息
struct ZenFSSnapshotOptions {
  // Global zoned device stats info zone设备信息
  bool zbd_ = 0;                     
  // Per zone stats info   每个zone的信息
  bool zone_ = 0;
  // Get all file->extents & extent->file mappings  获取文件与extent的双向映射关系
  bool zone_file_ = 0;
  bool trigger_report_ = 0;//触发器报告？
  bool log_garbage_ = 0;   //记录垃圾？
  bool as_lock_free_as_possible_ = 1; //尽可能地无锁。
};

class ZBDSnapshot {
 public:
  uint64_t free_space;          //空闲空间
  uint64_t used_space;          //使用的空间
  uint64_t reclaimable_space;   //可回收的空间

 public:
  ZBDSnapshot() = default;
  ZBDSnapshot(const ZBDSnapshot&) = default;
  ZBDSnapshot(ZonedBlockDevice& zbd)
      : free_space(zbd.GetFreeSpace()),
        used_space(zbd.GetUsedSpace()),
        reclaimable_space(zbd.GetReclaimableSpace()) {}
};

class ZoneSnapshot {
 public:
  uint64_t start;          //快照zone 起始地址
  uint64_t wp;             //快照zone 写指针位置

  uint64_t capacity;       //快照zone 容量
  uint64_t used_capacity;  //已经使用的容量
  uint64_t max_capacity;   //最大容量

 public:
  ZoneSnapshot(const Zone& zone)
      : start(zone.start_),
        wp(zone.wp_),
        capacity(zone.capacity_),
        used_capacity(zone.used_capacity_),
        max_capacity(zone.max_capacity_) {}
};

class ZoneExtentSnapshot {
 public:
  uint64_t start;          //zone extent的快照 起始地址
  uint64_t length;         //zone extent的快照 长度
  uint64_t zone_start;     //zone extent的快照 所属zone 的起始地址？
  std::string filename;    //zone extent的快照 所属file 的name

 public:
  ZoneExtentSnapshot(const ZoneExtent& extent, const std::string fname)
      : start(extent.start_),
        length(extent.length_),
        zone_start(extent.zone_->start_),
        filename(fname) {}
};

class ZoneFileSnapshot {
 public:
  uint64_t file_id;                       //zone file快照 fileid
  std::string filename;                   //zone file快照 filename
  std::vector<ZoneExtentSnapshot> extents;//zone file快照

 public:
  ZoneFileSnapshot(ZoneFile& file)
      : file_id(file.GetID()), filename(file.GetFilename()) {
    for (const auto* extent : file.GetExtents()) {
      extents.emplace_back(*extent, filename);
    }
  }
};

//zenfs的快照就包含了上面所有的快照  zdb快照  zone快照  zonefile快照 zonefileextent快照
class ZenFSSnapshot {
 public:
  ZenFSSnapshot() {}

  ZenFSSnapshot& operator=(ZenFSSnapshot&& snapshot) {
    zbd_ = snapshot.zbd_;
    zones_ = std::move(snapshot.zones_);
    zone_files_ = std::move(snapshot.zone_files_);
    extents_ = std::move(snapshot.extents_);
    return *this;
  }

 public:
  ZBDSnapshot zbd_;
  std::vector<ZoneSnapshot> zones_;
  std::vector<ZoneFileSnapshot> zone_files_;
  std::vector<ZoneExtentSnapshot> extents_;
};

}  // namespace ROCKSDB_NAMESPACE
