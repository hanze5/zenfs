// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Metrics Framework Introduction
//
// The metrics framework is used for users to identify ZenFS's performance
// bottomneck, it can collect throuput, qps and latency of each critical
// function call.
//
// For different RocksDB forks, users could custom their own metrics reporter to
// define how they would like to report these collected information.
//
// Steps to add new metrics trace point:
//   1. Add a new trace point label name in `ZenFSMetricsHistograms`.
//   2. Find target function, add these lines for tracing
//       // Latency Trace
//       ZenFSMetricsGuard guard(zoneFile_->GetZBDMetrics(),
//       ZENFS_WAL_WRITE_LATENCY, Env::Default());
//       // Throughput Trace
//       zoneFile_->GetZBDMetrics()->ReportThroughput(ZENFS_WRITE_THROUGHPUT,
//       data.size());
//       // QPS Trace
//       zoneFile_->GetZBDMetrics()->ReportQPS(ZENFS_WRITE_QPS, 1);
//    3. Implement a `ZenFSMetrics` to define how you would like to report your
//    data (Refer to file  `metrics_sample.h`)
//    4. Define your customized label name when implement ZenFSMetrics
//    5. Init your metrics and pass it into `NewZenFS()` function (default is
//    `NoZenFSMetrics`)

#pragma once
#include "rocksdb/env.h"
namespace ROCKSDB_NAMESPACE {

class ZenFSMetricsGuard;   //这个类可能用于跟踪特定函数的性能指标，如延迟、吞吐量和QPS
class ZenFSSnapshot;       //这个类可能用于获取zoned block device的状态快照，包括设备的空闲空间、已使用空间、可回收空间等信息
class ZenFSSnapshotOptions;//可能用于配置ZenFS的快照选项，如是否获取全局zoned device统计信息、每个区域的统计信息、所有文件到扩展和扩展到文件的映射等

// Types of Reporter that may be used for statistics.  以后这个是type
enum ZenFSMetricsReporterType : uint32_t {
  ZENFS_REPORTER_TYPE_WITHOUT_CHECK = 0,  //可能表示没有进行检查的报告器类型
  ZENFS_REPORTER_TYPE_GENERAL,            //一般
  ZENFS_REPORTER_TYPE_LATENCY,            //延迟
  ZENFS_REPORTER_TYPE_QPS,                //每秒查询率？  每秒请求数 可能是IOPS
  ZENFS_REPORTER_TYPE_THROUGHPUT,         //屯涂料
};

// Names of Reporter that may be used for statistics.  什么指标直方图 反正以后这个是label 
enum ZenFSMetricsHistograms : uint32_t {
  ZENFS_HISTOGRAM_ENUM_MIN,

  ZENFS_READ_LATENCY,             //读取延迟
  ZENFS_READ_QPS,                 //读取的查询每秒（QPS）

  ZENFS_WRITE_LATENCY,            //写延迟
  ZENFS_WAL_WRITE_LATENCY,        //写入日志（WAL）的延迟
  ZENFS_NON_WAL_WRITE_LATENCY,    //非日志写入的延迟。
  ZENFS_WRITE_QPS,                //写入的查询每秒（QPS）
  ZENFS_WRITE_THROUGHPUT,         //写入吞吐量。

  ZENFS_SYNC_LATENCY,             //同步延迟
  ZENFS_WAL_SYNC_LATENCY,         //日志同步延迟
  ZENFS_NON_WAL_SYNC_LATENCY,     //非日志同步延迟
  ZENFS_SYNC_QPS,                 //同步的查询每秒（QPS）

  ZENFS_IO_ALLOC_LATENCY,         //IO分配延迟
  ZENFS_WAL_IO_ALLOC_LATENCY,     //日志IO分配延迟
  ZENFS_NON_WAL_IO_ALLOC_LATENCY, //非日志IO分配延迟
  ZENFS_IO_ALLOC_QPS,             //IO分配的查询每秒（QPS）

  ZENFS_META_ALLOC_LATENCY,       //元数据分配延迟
  ZENFS_META_ALLOC_QPS,           //元数据分配的查询每秒（QPS）

  ZENFS_META_SYNC_LATENCY,        //元数据同步延迟。

  ZENFS_ROLL_LATENCY,             //滚动延迟
  ZENFS_ROLL_QPS,                 //滚动的查询每秒（QPS）
  ZENFS_ROLL_THROUGHPUT,          //滚动吞吐量

  ZENFS_ACTIVE_ZONES_COUNT,       //活动区域计数
  ZENFS_OPEN_ZONES_COUNT,         //打开区域计数

  ZENFS_FREE_SPACE_SIZE,          //空闲空间大小
  ZENFS_USED_SPACE_SIZE,          //已使用空间大小
  ZENFS_RECLAIMABLE_SPACE_SIZE,   //可回收空间大小

  ZENFS_RESETABLE_ZONES_COUNT,    //可重置区域计数

  ZENFS_HISTOGRAM_ENUM_MAX,       //

  ZENFS_ZONE_WRITE_THROUGHPUT,    //区域写入吞吐量。
  ZENFS_ZONE_WRITE_LATENCY,       //区域写入延迟

  ZENFS_L0_IO_ALLOC_LATENCY,      //L0 IO分配延迟
};

//一个公共的抽象类，用于报告和跟踪 ZenFS 的性能指标
struct ZenFSMetrics {
 public:
  //用于标识报告器和报告器类型
  typedef uint32_t Label;
  typedef uint32_t ReporterType;
  // We give an enum to identify the reporters and an enum to identify the
  // reporter types: ZenFSMetricsHistograms and ZenFSMetricsReporterType,
  // respectively, at the end of the code.
  /**
   * 定义了两个枚举类型，一个用于标识报告器（ZenFSMetricsHistograms），另一个用于标识报告器类型（ZenFSMetricsReporterType）。
   * 这两个枚举类型分别在代码的后面定义。这样做的目的是为了方便识别和管理不同的报告器和报告器类型，使得代码更加清晰和易于理解
  */
 public:
  ZenFSMetrics() {}
  virtual ~ZenFSMetrics() {}

 public:
  // Add a reporter named label.
  // You can give a type for type-checking.
  // 这是一个纯虚函数，用于添加一个名为 label 的报告器。你可以为它提供一个类型进行类型检查
  virtual void AddReporter(Label label, ReporterType type = 0) = 0;
  // Report a value for the reporter named label.
  // You can give a type for type-checking.
  // 这是一个纯虚函数，用于报告名为 label 的报告器的值。你可以为它提供一个类型进行类型检查。
  virtual void Report(Label label, size_t value,
                      ReporterType type_check = 0) = 0;
  //纯虚函数，用于报告 ZenFSSnapshot 的快照
  virtual void ReportSnapshot(const ZenFSSnapshot& snapshot) = 0;

 public:
  // Syntactic sugars for type-checking.
  // Overwrite them if you think type-checking is necessary.
  // 这些都是虚函数，用于报告每秒查询率（QPS）、吞吐量、延迟和一般数据。你可以重写它们，如果你认为类型检查是必要的。
  virtual void ReportQPS(Label label, size_t qps) { Report(label, qps, 0); }
  virtual void ReportThroughput(Label label, size_t throughput) {
    Report(label, throughput, 0);
  }
  virtual void ReportLatency(Label label, size_t latency) {
    Report(label, latency, 0);
  }
  virtual void ReportGeneral(Label label, size_t data) {
    Report(label, data, 0);
  }

  // and more
};

struct NoZenFSMetrics : public ZenFSMetrics {
  NoZenFSMetrics() : ZenFSMetrics() {}
  virtual ~NoZenFSMetrics() {}

 public:
  virtual void AddReporter(uint32_t /*label*/, uint32_t /*type*/) override {}
  virtual void Report(uint32_t /*label*/, size_t /*value*/,
                      uint32_t /*type_check*/) override {}
  virtual void ReportSnapshot(const ZenFSSnapshot& /*snapshot*/) override {}
};


// The implementation of this class will start timing when initialized,
// stop timing when it is destructured,
// and report the difference in time to the target label via
// metrics->ReportLatency(). By default, the method to collect the time will be
// to call env->NowMicros().
/**
 * 这个类的实现：
 * 在初始化时开始计时，在析构时停止计时，并通过 metrics->ReportLatency() 报告时间差。
 * 默认情况下，收集时间的方法将是调用 env->NowMicros()。
 * 换句话说，这个类被设计为一个计时器，它在创建时开始计时，在销毁时结束计时，并将计时结果报告给指定的标签
 * 
 * 这就是为什么被叫做卫兵了  就只有构造和析构   分别表示开始与结束计时
*/
struct ZenFSMetricsLatencyGuard {
  std::shared_ptr<ZenFSMetrics> metrics_;
  uint32_t label_;
  Env* env_;
  uint64_t begin_time_micro_;

  ZenFSMetricsLatencyGuard(std::shared_ptr<ZenFSMetrics> metrics,
                           uint32_t label, Env* env)
      : metrics_(metrics),
        label_(label),
        env_(env),
        begin_time_micro_(GetTime()) {}

  virtual ~ZenFSMetricsLatencyGuard() {
    uint64_t end_time_micro_ = GetTime();
    assert(end_time_micro_ >= begin_time_micro_);
    metrics_->ReportLatency(label_,
                            Report(end_time_micro_ - begin_time_micro_));
  }
  // overwrite this function if you wish to capture time by other methods.这里可以重写以更换获取时间的方法
  virtual uint64_t GetTime() { return env_->NowMicros(); }
  // overwrite this function if you do not intend to report delays measured in
  // microseconds.  这里可以重写以改变单位
  virtual uint64_t Report(uint64_t time) { return time; }
};

#define ZENFS_LABEL(label, type) ZENFS_##label##_##type
#define ZENFS_LABEL_DETAILED(label, sub_label, type) \
  ZENFS_##sub_label##_##label##_##type
// eg : ZENFS_LABEL(WRITE, WAL, THROUGHPUT) => ZENFS_WAL_WRITE_THROUGHPUT

}  // namespace ROCKSDB_NAMESPACE
