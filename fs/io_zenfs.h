// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

/**
 * ZenFS则采用了划分extent方式来对目标文件进行切片管理，每个extent切片对应Zone内一段物理连续的空间(extent不会跨Zone进行存储)，extent列表则记录到每个文件对应的ZoneFile实例(类似内核的inode)
*/
class ZoneExtent { 
 public:
  uint64_t start_;
  uint64_t length_;
  Zone* zone_;

  explicit ZoneExtent(uint64_t start, uint64_t length, Zone* zone);
  Status DecodeFrom(Slice* input);//这个方法用于从一个 Slice 对象中解码一个 ZoneExtent 对象。如果解码成功，它会返回一个 Status 对象，表示操作的状态
  void EncodeTo(std::string* output);
  void EncodeJson(std::ostream& json_stream);
};

class ZoneFile;

/* Interface for persisting metadata for files 一个名为 MetadataWriter 的类，它是一个接口，用于持久化文件的元数据*/
class MetadataWriter {
 public:
  virtual ~MetadataWriter();
  virtual IOStatus Persist(ZoneFile* zoneFile) = 0;
};

class ZoneFile {
 private:
  const uint64_t NO_EXTENT = 0xffffffffffffffff;

  ZonedBlockDevice* zbd_;             //表示一个区块设备

  std::vector<ZoneExtent*> extents_;  //表示zonefile包含的extent
  std::vector<std::string> linkfiles_;//可能表示一系列的链接文件

  Zone* active_zone_;                 //示当前活动的区域 文件所属区域？   
  uint64_t extent_start_ = NO_EXTENT; //表示extent的起始位置。NO_EXTENT 可能是一个特殊的值，表示没有范围。
  uint64_t extent_filepos_ = 0;       //extent在文件中的位置

  Env::WriteLifeTimeHint lifetime_;   //写入操作的生命周期
  IOType io_type_; /* Only used when writing */
  uint64_t file_size_;                 
  uint64_t file_id_;

  uint32_t nr_synced_extents_ = 0;     //已同步的范围数量，初始值为0
  bool open_for_wr_ = false;           //是否打开了写入权限，初始值为false
  std::mutex open_for_wr_mtx_;         //

  time_t m_time_;                      //文件上次被修改的时间
  bool is_sparse_ = false;             //文件是否是稀疏文件，初始值为false
  bool is_deleted_ = false;            //文件是否已被删除，初始值为false

  MetadataWriter* metadata_writer_ = NULL;//用于写入文件的元数据，初始值为NULL

  std::mutex writer_mtx_;              //用于保护 metadata_writer_ 的访问
  std::atomic<int> readers_{0};        //当前读取文件的读取器数量，初始值为0

 public:
  static const int SPARSE_HEADER_SIZE = 8;//可能表示的是稀疏文件头部信息的大小

  
  //dz modified
  explicit ZoneFile(ZonedBlockDevice* zbd, uint64_t file_id_,
                    MetadataWriter* metadata_writer);

  virtual ~ZoneFile();

  void AcquireWRLock();            //获取写锁
  bool TryAcquireWRLock();         //尝试获取写锁
  void ReleaseWRLock();            //释放写锁

  IOStatus CloseWR();              //关闭写操作 包括：持久化元数据 释放写锁  关闭当前activezone
  bool IsOpenForWR();              //判断是否获取了写入权限

  IOStatus PersistMetadata();      //用于持久化元数据

  IOStatus Append(void* buffer, int data_size);//本质是调用  AllocateNewZone() 与 zone->Append()
  IOStatus BufferedAppend(char* data, uint32_t size);//
  IOStatus SparseAppend(char* data, uint32_t size);//以字节对齐的方式进行稀疏写入，同时在内联元数据中预留8字节的空间用于大小头部
  IOStatus SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime);////设置zonefile的生命周期
  void SetIOType(IOType io_type);
  std::string GetFilename();           //返回linkfiles_[0]
  time_t GetFileModificationTime();    //获取文件修改时间
  void SetFileModificationTime(time_t mt);
  uint64_t GetFileSize();              //获取文件大小
  void SetFileSize(uint64_t sz);       //设置文件大小
  void ClearExtents();                 //清除所有文件的extents

  uint32_t GetBlockSize() { return zbd_->GetBlockSize(); }
  ZonedBlockDevice* GetZbd() { return zbd_; }
  std::vector<ZoneExtent*> GetExtents() { return extents_; }
  Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return lifetime_; }

  IOStatus PositionedRead(uint64_t offset, size_t n, Slice* result,
                          char* scratch, bool direct);//从指定的偏移量开始读取一定数量的数据 本质是循环调用 zbd_->Read
  ZoneExtent* GetExtent(uint64_t file_offset, uint64_t* dev_offset);//获取与给定文件偏移量对应的extent
  void PushExtent(); //它用于将新的extent添加到extents_列表中 一般就是有新数据要写入需要开辟空间了
  IOStatus AllocateNewZone();

  IOStatus AllocateNewZone(std::string filename);

  void EncodeTo(std::string* output, uint32_t extent_start);//它将ZoneFile对象的状态编码到一个字符串中
  void EncodeUpdateTo(std::string* output) {
    EncodeTo(output, nr_synced_extents_);
  };
  void EncodeSnapshotTo(std::string* output) { EncodeTo(output, 0); };//调用EncodeTo函数将ZoneFile对象的状态编码到一个字符串中
  void EncodeJson(std::ostream& json_stream);//它将ZoneFile对象的状态编码为JSON格式
  void MetadataSynced() { nr_synced_extents_ = extents_.size(); };
  void MetadataUnsynced() { nr_synced_extents_ = 0; };

  IOStatus MigrateData(uint64_t offset, uint32_t length, Zone* target_zone);

  Status DecodeFrom(Slice* input);//从输入的Slice对象中解码ZoneFile对象的状态
  Status MergeUpdate(std::shared_ptr<ZoneFile> update, bool replace);//用于将更新的ZoneFile对象的状态合并到当前对象中

  uint64_t GetID() { return file_id_; }

  bool IsSparse() { return is_sparse_; };

  void SetSparse(bool is_sparse) { is_sparse_ = is_sparse; };
  uint64_t HasActiveExtent() { return extent_start_ != NO_EXTENT; };
  uint64_t GetExtentStart() { return extent_start_; };

  IOStatus Recover();

  void ReplaceExtentList(std::vector<ZoneExtent*> new_list);//接受一个ZoneExtent对象的向量new_list作为参数。这个函数的主要功能是替换ZoneFile对象的extents_成员变量
  void AddLinkName(const std::string& linkfile);////加入一个新的LinkName
  IOStatus RemoveLinkName(const std::string& linkfile);////把名为src的linkfile重命名为dest
  IOStatus RenameLink(const std::string& src, const std::string& dest);////删除名为 <参数> 的linkfile
  uint32_t GetNrLinks() { return linkfiles_.size(); }  //
  const std::vector<std::string>& GetLinkFiles() const { return linkfiles_; }

  IOStatus InvalidateCache(uint64_t pos, uint64_t size);    //使缓存失效

 private:
  void ReleaseActiveZone();
  void SetActiveZone(Zone* zone);
  IOStatus CloseActiveZone();            //关闭当前活动的区域

 public:
  std::shared_ptr<ZenFSMetrics> GetZBDMetrics() { return zbd_->GetMetrics(); };
  IOType GetIOType() const { return io_type_; };
  bool IsDeleted() const { return is_deleted_; };
  void SetDeleted() { is_deleted_ = true; };
  IOStatus RecoverSparseExtents(uint64_t start, uint64_t end, Zone* zone);

 public:
  class ReadLock {
   public:
    ReadLock(ZoneFile* zfile) : zfile_(zfile) {
      zfile_->writer_mtx_.lock();
      zfile_->readers_++;
      zfile_->writer_mtx_.unlock();
    }
    ~ReadLock() { zfile_->readers_--; }

   private:
    ZoneFile* zfile_;
  };


  class WriteLock {
   public:
    WriteLock(ZoneFile* zfile) : zfile_(zfile) {
      zfile_->writer_mtx_.lock();
      while (zfile_->readers_ > 0) {
      }
    }
    ~WriteLock() { zfile_->writer_mtx_.unlock(); }

   private:
    ZoneFile* zfile_;
  };
};

class ZonedWritableFile : public FSWritableFile {
 public:
  explicit ZonedWritableFile(ZonedBlockDevice* zbd, bool buffered,
                             std::shared_ptr<ZoneFile> zoneFile);
  virtual ~ZonedWritableFile();

  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override;
  virtual IOStatus PositionedAppend(
      const Slice& data, uint64_t offset, const IOOptions& opts,
      const DataVerificationInfo& /* verification_info */,
      IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }
  virtual IOStatus Truncate(uint64_t size, const IOOptions& options,
                            IODebugContext* dbg) override;//将文件截断到指定的大小
  virtual IOStatus Close(const IOOptions& options,
                         IODebugContext* dbg) override;//关闭文件
  virtual IOStatus Flush(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  virtual IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                             const IOOptions& options,
                             IODebugContext* dbg) override;//同步文件的指定范围
  virtual IOStatus Fsync(const IOOptions& options,
                         IODebugContext* dbg) override;

  bool use_direct_io() const override { return !buffered; } //如果不启用缓存 将返回真
  bool IsSyncThreadSafe() const override { return true; };  //同步操作是否线程安全
  size_t GetRequiredBufferAlignment() const override {      //返回所需的缓冲区对其大小 这里返回的是zonefile->blocksize
    return zoneFile_->GetBlockSize();
  }
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;////设置zonefile的生命周期
  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return zoneFile_->GetWriteLifeTimeHint();
  }

 private:
  IOStatus BufferedWrite(const Slice& data);//将数据写入缓冲区。
  IOStatus FlushBuffer();                   //调用DataSync()将缓冲区数据刷新到磁盘
  IOStatus DataSync();                      //同步数据到磁盘
  IOStatus CloseInternal();                 //关闭文件

  bool buffered;                            //是否使用缓冲区写入
  char* sparse_buffer;                      //稀疏缓冲区指针
  char* buffer;                             //缓冲区指针
  size_t buffer_sz;                         //缓冲区大小
  uint32_t block_sz;                        //块大小
  uint32_t buffer_pos;                      //缓冲区当前位置
  uint64_t wp;                              //写指针位置
  int write_temp;                           //临时写入的变量
  bool open;                                //文件是否打开

  std::shared_ptr<ZoneFile> zoneFile_;       //一个指向ZoneFile对象的共享指针。
  MetadataWriter* metadata_writer_;          //一个指向MetadataWriter对象的指针

  std::mutex buffer_mtx_;                    //一个互斥锁，用于保护缓冲区的并发访问。
};

class ZonedSequentialFile : public FSSequentialFile {
 private:
  std::shared_ptr<ZoneFile> zoneFile_;
  uint64_t rp;
  bool direct_;

 public:
  explicit ZonedSequentialFile(std::shared_ptr<ZoneFile> zoneFile,
                               const FileOptions& file_opts)
      : zoneFile_(zoneFile),
        rp(0),
        direct_(file_opts.use_direct_reads && !zoneFile->IsSparse()) {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
  IOStatus Skip(uint64_t n) override;

  bool use_direct_io() const override { return direct_; };

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return zoneFile_->InvalidateCache(offset, length);
  }
};

class ZonedRandomAccessFile : public FSRandomAccessFile {
 private:
  std::shared_ptr<ZoneFile> zoneFile_;
  bool direct_;

 public:
  explicit ZonedRandomAccessFile(std::shared_ptr<ZoneFile> zoneFile,
                                 const FileOptions& file_opts)
      : zoneFile_(zoneFile),
        direct_(file_opts.use_direct_reads && !zoneFile->IsSparse()) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Prefetch(uint64_t /*offset*/, size_t /*n*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  bool use_direct_io() const override { return direct_; }

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return zoneFile_->InvalidateCache(offset, length);
  }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
