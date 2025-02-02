// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if __cplusplus < 201703L
#include "filesystem_utility.h"
namespace fs = filesystem_utility;
#else
#include <filesystem>
namespace fs = std::filesystem;
#endif

#include <memory>
#include <thread>

#include "io_zenfs.h"
#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "snapshot.h"
#include "version.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

class ZoneSnapshot;
class ZoneFileSnapshot;
class ZenFSSnapshot;
class ZenFSSnapshotOptions;

class Superblock {
  uint32_t magic_ = 0;               //用于验证超级快完整性或者识别文件系统类型
  char uuid_[37] = {0};              //文件系统唯一标识
  uint32_t sequence_ = 0;            //
  uint32_t superblock_version_ = 0;  //超级快版本
  uint32_t flags_ = 0;               
  uint32_t block_size_ = 0; /* in bytes */
  uint32_t zone_size_ = 0;  /* in blocks */
  uint32_t nr_zones_ = 0;
  char aux_fs_path_[256] = {0};      //辅助文件系统的路径？  反正当时用于存放日志
  uint32_t finish_treshold_ = 0;     
  char zenfs_version_[64]{0};        //zenfs版本
  char reserved_[123] = {0};         //保留位

 public:
  const uint32_t MAGIC = 0x5a454e46; /* ZENF 标识和验证文件系统*/
  const uint32_t ENCODED_SIZE = 512; ///超级快编码大小 512B
  const uint32_t CURRENT_SUPERBLOCK_VERSION = 2;//当前超级快版本是2
  const uint32_t DEFAULT_FLAGS = 0;            
  const uint32_t FLAGS_ENABLE_GC = 1 << 0;      //启用或禁用垃圾收集

  Superblock() {}

  /* Create a superblock for a filesystem covering the entire zoned block device
   */
  Superblock(ZonedBlockDevice* zbd, std::string aux_fs_path = "",
             uint32_t finish_threshold = 0, bool enable_gc = false) {
    std::string uuid = Env::Default()->GenerateUniqueId();
    int uuid_len =
        std::min(uuid.length(),
                 sizeof(uuid_) - 1); /* make sure uuid is nullterminated */
    memcpy((void*)uuid_, uuid.c_str(), uuid_len);
    magic_ = MAGIC;
    superblock_version_ = CURRENT_SUPERBLOCK_VERSION;
    flags_ = DEFAULT_FLAGS;
    if (enable_gc) flags_ |= FLAGS_ENABLE_GC;

    finish_treshold_ = finish_threshold;

    block_size_ = zbd->GetBlockSize();
    zone_size_ = zbd->GetZoneSize() / block_size_;
    nr_zones_ = zbd->GetNrZones();

    strncpy(aux_fs_path_, aux_fs_path.c_str(), sizeof(aux_fs_path_) - 1);

    std::string zenfs_version = ZENFS_VERSION;
    strncpy(zenfs_version_, zenfs_version.c_str(), sizeof(zenfs_version_) - 1);
  }

  Status DecodeFrom(Slice* input);//主要功能是从输入的 Slice 对象中解码超级块的信息
  void EncodeTo(std::string* output);//将超级块的信息编码到输出的 std::string 对象中
  Status CompatibleWith(ZonedBlockDevice* zbd);//检查 ZenFS 超级块的一些属性是否与 zbd 兼容

  void GetReport(std::string* reportString);//报告超级快信息

  uint32_t GetSeq() { return sequence_; }
  std::string GetAuxFsPath() { return std::string(aux_fs_path_); }
  uint32_t GetFinishTreshold() { return finish_treshold_; }
  std::string GetUUID() { return std::string(uuid_); }
  bool IsGCEnabled() { return flags_ & FLAGS_ENABLE_GC; };
};

class ZenMetaLog {
  uint64_t read_pos_;
  Zone* zone_;
  ZonedBlockDevice* zbd_;
  size_t bs_;

  /* Every meta log record is prefixed with a CRC(32 bits) and record length (32
   * bits) */
  const size_t zMetaHeaderSize = sizeof(uint32_t) * 2;

 public:
  ZenMetaLog(ZonedBlockDevice* zbd, Zone* zone) {
    assert(zone->IsBusy());
    zbd_ = zbd;
    zone_ = zone;
    bs_ = zbd_->GetBlockSize();
    read_pos_ = zone->start_;
  }

  virtual ~ZenMetaLog() {
    // TODO: report async error status
    bool ok = zone_->Release();
    assert(ok);
    (void)ok;
  }

  IOStatus AddRecord(const Slice& slice);////将 slice 中的数据添加到元数据日志中
  IOStatus ReadRecord(Slice* record, std::string* scratch);//

  Zone* GetZone() { return zone_; };

 private:
  IOStatus Read(Slice* slice);  //从元数据日志中读取数据
};

//最为重要的组件集成者    zenfs！！
class ZenFS : public FileSystemWrapper {
  ZonedBlockDevice* zbd_;                                 //zenfs所属zbd
  std::map<std::string, std::shared_ptr<ZoneFile>> files_;//文件名到Zonefile的映射合集
  std::mutex files_mtx_;                                  //访问file的锁
  std::shared_ptr<Logger> logger_;                        //记录日志
  std::atomic<uint64_t> next_file_id_;                    //原子变量 用于生成下一个文件的id？

  Zone* cur_meta_zone_ = nullptr;                         //Zone类型的指针 可能用于访问或操作当前元区域
  std::unique_ptr<ZenMetaLog> meta_log_;                  //ZenMetaLog类型的独特指针，可能用于访问或操作元日志。
  std::mutex metadata_sync_mtx_;                          //互斥锁，可能用于同步元数据的访问
  std::unique_ptr<Superblock> superblock_;                //指向Superblock类型的独特指针，可能用于访问或操作超级块

  std::shared_ptr<Logger> GetLogger() { return logger_; } //返回logger_的共享指针

  std::unique_ptr<std::thread> gc_worker_ = nullptr;      //这是一个指向std::thread类型的独特指针，用于执行垃圾收集工作
  bool run_gc_worker_ = false;                            //布尔值，用于控制是否运行垃圾收集工作

  struct ZenFSMetadataWriter : public MetadataWriter {   
    ZenFS* zenFS;
    IOStatus Persist(ZoneFile* zoneFile) {
      Debug(zenFS->GetLogger(), "Syncing metadata for: %s",
            zoneFile->GetFilename().c_str());
      return zenFS->SyncFileMetadata(zoneFile);
    }
  };

  ZenFSMetadataWriter metadata_writer_;                   // //zenfs用于写元数据的结构体

  //表示ZenFS文件系统中的不同标签。每个标签都对应一个uint32_t类型的值
  enum ZenFSTag : uint32_t {
    kCompleteFilesSnapshot = 1,  //完整文件快照
    kFileUpdate = 2,             //文件更新
    kFileDeletion = 3,           //文件删除
    kEndRecord = 4,              //记录结束
    kFileReplace = 5,            //文件替换
  };

  void LogFiles();
  void ClearFiles();
  std::string FormatPathLexically(fs::path filepath);
  IOStatus WriteSnapshotLocked(ZenMetaLog* meta_log);
  IOStatus WriteEndRecord(ZenMetaLog* meta_log);
  IOStatus RollMetaZoneLocked();
  IOStatus PersistSnapshot(ZenMetaLog* meta_writer);
  IOStatus PersistRecord(std::string record);
  IOStatus SyncFileExtents(ZoneFile* zoneFile,
                           std::vector<ZoneExtent*> new_extents);
  /* Must hold files_mtx_ */
  IOStatus SyncFileMetadataNoLock(ZoneFile* zoneFile, bool replace = false);
  /* Must hold files_mtx_ */
  IOStatus SyncFileMetadataNoLock(std::shared_ptr<ZoneFile> zoneFile,
                                  bool replace = false) {
    return SyncFileMetadataNoLock(zoneFile.get(), replace);
  }
  IOStatus SyncFileMetadata(ZoneFile* zoneFile, bool replace = false);
  IOStatus SyncFileMetadata(std::shared_ptr<ZoneFile> zoneFile,
                            bool replace = false) {
    return SyncFileMetadata(zoneFile.get(), replace);
  }

  void EncodeSnapshotTo(std::string* output);
  void EncodeFileDeletionTo(std::shared_ptr<ZoneFile> zoneFile,
                            std::string* output, std::string linkf);

  Status DecodeSnapshotFrom(Slice* input);
  Status DecodeFileUpdateFrom(Slice* slice, bool replace = false);
  Status DecodeFileDeletionFrom(Slice* slice);

  Status RecoverFrom(ZenMetaLog* log);

  std::string ToAuxPath(std::string path) {
    return superblock_->GetAuxFsPath() + path;
  }

  std::string ToZenFSPath(std::string aux_path) {
    std::string path = aux_path;
    path.erase(0, superblock_->GetAuxFsPath().length());
    return path;
  }

  /* Must hold files_mtx_ */
  std::shared_ptr<ZoneFile> GetFileNoLock(std::string fname);
  /* Must hold files_mtx_ */
  void GetZenFSChildrenNoLock(const std::string& dir,
                              bool include_grandchildren,
                              std::vector<std::string>* result);
  /* Must hold files_mtx_ */
  IOStatus GetChildrenNoLock(const std::string& dir, const IOOptions& options,
                             std::vector<std::string>* result,
                             IODebugContext* dbg);

  /* Must hold files_mtx_ */
  IOStatus RenameChildNoLock(std::string const& source_dir,
                             std::string const& dest_dir,
                             std::string const& child, const IOOptions& options,
                             IODebugContext* dbg);

  /* Must hold files_mtx_ */
  IOStatus RollbackAuxDirRenameNoLock(
      const std::string& source_path, const std::string& dest_path,
      const std::vector<std::string>& renamed_children,
      const IOOptions& options, IODebugContext* dbg);

  /* Must hold files_mtx_ */
  IOStatus RenameAuxPathNoLock(const std::string& source_path,
                               const std::string& dest_path,
                               const IOOptions& options, IODebugContext* dbg);

  /* Must hold files_mtx_ */
  IOStatus RenameFileNoLock(const std::string& f, const std::string& t,
                            const IOOptions& options, IODebugContext* dbg);

  std::shared_ptr<ZoneFile> GetFile(std::string fname);

  /* Must hold files_mtx_, On successful return,
   * caller must release files_mtx_ and call ResetUnusedIOZones() */
  IOStatus DeleteFileNoLock(std::string fname, const IOOptions& options,
                            IODebugContext* dbg);

  IOStatus Repair();

  /* Must hold files_mtx_ */
  IOStatus DeleteDirRecursiveNoLock(const std::string& d,
                                    const IOOptions& options,
                                    IODebugContext* dbg);

  /* Must hold files_mtx_ */
  IOStatus IsDirectoryNoLock(const std::string& path, const IOOptions& options,
                             bool* is_dir, IODebugContext* dbg) {
    if (GetFileNoLock(path) != nullptr) {
      *is_dir = false;
      return IOStatus::OK();
    }
    return target()->IsDirectory(ToAuxPath(path), options, is_dir, dbg);
  }

 protected:
  IOStatus OpenWritableFile(const std::string& fname,
                            const FileOptions& file_opts,
                            std::unique_ptr<FSWritableFile>* result,
                            IODebugContext* dbg, bool reopen);

 public:
  explicit ZenFS(ZonedBlockDevice* zbd, std::shared_ptr<FileSystem> aux_fs,
                 std::shared_ptr<Logger> logger);
  virtual ~ZenFS();

  Status Mount(bool readonly);
  Status MkFS(std::string aux_fs_path, uint32_t finish_threshold,
              bool enable_gc);
  std::map<std::string, Env::WriteLifeTimeHint> GetWriteLifeTimeHints();

  const char* Name() const override {
    return "ZenFS - The Zoned-enabled File System";
  }

  void EncodeJson(std::ostream& json_stream);

  void ReportSuperblock(std::string* report) { superblock_->GetReport(report); }

  virtual IOStatus NewSequentialFile(const std::string& fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSSequentialFile>* result,
                                     IODebugContext* dbg) override;
  virtual IOStatus NewRandomAccessFile(
      const std::string& fname, const FileOptions& file_opts,
      std::unique_ptr<FSRandomAccessFile>* result,
      IODebugContext* dbg) override;
  virtual IOStatus NewWritableFile(const std::string& fname,
                                   const FileOptions& file_opts,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) override;
  virtual IOStatus ReuseWritableFile(const std::string& fname,
                                     const std::string& old_fname,
                                     const FileOptions& file_opts,
                                     std::unique_ptr<FSWritableFile>* result,
                                     IODebugContext* dbg) override;
  virtual IOStatus ReopenWritableFile(const std::string& fname,
                                      const FileOptions& options,
                                      std::unique_ptr<FSWritableFile>* result,
                                      IODebugContext* dbg) override;
  virtual IOStatus FileExists(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  virtual IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                               std::vector<std::string>* result,
                               IODebugContext* dbg) override;
  virtual IOStatus DeleteFile(const std::string& fname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;
  virtual IOStatus LinkFile(const std::string& fname, const std::string& lname,
                            const IOOptions& options,
                            IODebugContext* dbg) override;
  virtual IOStatus NumFileLinks(const std::string& fname,
                                const IOOptions& options, uint64_t* nr_links,
                                IODebugContext* dbg) override;
  virtual IOStatus AreFilesSame(const std::string& fname,
                                const std::string& link,
                                const IOOptions& options, bool* res,
                                IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                       uint64_t* size, IODebugContext* dbg) override;
  IOStatus RenameFile(const std::string& f, const std::string& t,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus GetFreeSpace(const std::string& /*path*/,
                        const IOOptions& /*options*/, uint64_t* diskfree,
                        IODebugContext* /*dbg*/) override {
    *diskfree = zbd_->GetFreeSpace();
    return IOStatus::OK();
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options, uint64_t* mtime,
                                   IODebugContext* dbg) override;

  // The directory structure is stored in the aux file system

  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override {
    std::lock_guard<std::mutex> lock(files_mtx_);
    return IsDirectoryNoLock(path, options, is_dir, dbg);
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    Debug(logger_, "NewDirectory: %s to aux: %s\n", name.c_str(),
          ToAuxPath(name).c_str());
    return target()->NewDirectory(ToAuxPath(name), io_opts, result, dbg);
  }

  IOStatus CreateDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    Debug(logger_, "CreatDir: %s to aux: %s\n", d.c_str(),
          ToAuxPath(d).c_str());
    return target()->CreateDir(ToAuxPath(d), options, dbg);
  }

  IOStatus CreateDirIfMissing(const std::string& d, const IOOptions& options,
                              IODebugContext* dbg) override {
    Debug(logger_, "CreatDirIfMissing: %s to aux: %s\n", d.c_str(),
          ToAuxPath(d).c_str());
    return target()->CreateDirIfMissing(ToAuxPath(d), options, dbg);
  }

  IOStatus DeleteDir(const std::string& d, const IOOptions& options,
                     IODebugContext* dbg) override {
    std::vector<std::string> children;
    IOStatus s;

    Debug(logger_, "DeleteDir: %s aux: %s\n", d.c_str(), ToAuxPath(d).c_str());

    s = GetChildren(d, options, &children, dbg);
    if (children.size() != 0)
      return IOStatus::IOError("Directory has children");

    return target()->DeleteDir(ToAuxPath(d), options, dbg);
  }

  IOStatus DeleteDirRecursive(const std::string& d, const IOOptions& options,
                              IODebugContext* dbg);

  // We might want to override these in the future
  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    return target()->GetAbsolutePath(ToAuxPath(db_path), options, output_path,
                                     dbg);
  }

  IOStatus LockFile(const std::string& f, const IOOptions& options,
                    FileLock** l, IODebugContext* dbg) override {
    return target()->LockFile(ToAuxPath(f), options, l, dbg);
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& options,
                      IODebugContext* dbg) override {
    return target()->UnlockFile(l, options, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override {
    *path = "rocksdbtest";
    Debug(logger_, "GetTestDirectory: %s aux: %s\n", path->c_str(),
          ToAuxPath(*path).c_str());
    return target()->CreateDirIfMissing(ToAuxPath(*path), options, dbg);
  }

  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override {
    return target()->NewLogger(ToAuxPath(fname), options, result, dbg);
  }

  // Not supported (at least not yet)
  IOStatus Truncate(const std::string& /*fname*/, size_t /*size*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("Truncate is not implemented in ZenFS");
  }

  virtual IOStatus NewRandomRWFile(const std::string& /*fname*/,
                                   const FileOptions& /*options*/,
                                   std::unique_ptr<FSRandomRWFile>* /*result*/,
                                   IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("RandomRWFile is not implemented in ZenFS");
  }

  virtual IOStatus NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) override {
    return IOStatus::NotSupported(
        "MemoryMappedFileBuffer is not implemented in ZenFS");
  }

  void GetZenFSSnapshot(ZenFSSnapshot& snapshot,
                        const ZenFSSnapshotOptions& options);

  IOStatus MigrateExtents(const std::vector<ZoneExtentSnapshot*>& extents);

  IOStatus MigrateFileExtents(
      const std::string& fname,
      const std::vector<ZoneExtentSnapshot*>& migrate_exts);

 private:
  const uint64_t GC_START_LEVEL =
      20;                      /* Enable GC when < 20% free space available */
  const uint64_t GC_SLOPE = 3; /* GC agressiveness */
  void GCWorker();
};
#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)

Status NewZenFS(
    FileSystem** fs, const std::string& bdevname,
    std::shared_ptr<ZenFSMetrics> metrics = std::make_shared<NoZenFSMetrics>());
Status NewZenFS(
    FileSystem** fs, const ZbdBackendType backend_type,
    const std::string& backend_name,
    std::shared_ptr<ZenFSMetrics> metrics = std::make_shared<NoZenFSMetrics>());
Status AppendZenFileSystem(
    std::string path, ZbdBackendType backend,
    std::map<std::string, std::pair<std::string, ZbdBackendType>>& fs_list);
Status ListZenFileSystems(
    std::map<std::string, std::pair<std::string, ZbdBackendType>>& out_list);

}  // namespace ROCKSDB_NAMESPACE
