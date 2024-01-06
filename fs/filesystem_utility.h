//  SPDX-License-Identifier: Apache License 2.0 OR GPL-2.0
//
//  SPDX-FileCopyrightText: 2022, Western Digital Corporation or its affiliates.
//  Written by Kuankuan Guo <guokuankuan@bytedance.com>
//

#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// Utility filesystem path implementation
namespace filesystem_utility {
class path {
 public:
  path(const std::string& src_path) : src_path_(src_path) {
    lexically_normal();
  }
  path() { path(""); }
  std::string string() const { return normalized_path_; }
  operator std::string() const { return string(); }

  // Normalize the input path, keep the root slash and end
  // terminator, example:
  //
  // `//a/b/c    ->   /a/b/c`
  // `a/b/../c/  ->   a/c/`
  // `a/b/../c   ->   a/c`
  path lexically_normal() {
    //如果已经规范化那么就直接返回
    if (!normalized_path_.empty()) {
      return *this;
    }

    //如果源路径为空，那么规范化的路径也设置为空
    if (src_path_.empty()) {
      normalized_path_ = "";
      return *this;
    }
    //如果源路径只有一个根目录(/)，那么规范化的路径就是源路径
    if (src_path_.compare("/") == 0) {
      normalized_path_ = src_path_;
      has_terminator_ = true;
      return *this;
    }
    //函数会将源路径分割成多个段，并存储在一个向量中。在这个过程中，如果遇到..，则删除上一个段；如果遇到.，则忽略；如果遇到空字符串，也忽略
    std::stringstream rst;
    std::vector<std::string> segs;
    //函数会检查路径是否以/结尾，以确定是否有终结符。如果没有终结符，那么最后一个段就是文件名
    std::istringstream iss(src_path_);
    std::string item;
    while (std::getline(iss, item, '/')) {
      if (item == ".." && segs.size() > 0) {
        segs.pop_back();
        continue;
      }

      if (item == ".") {
        continue;
      }
      if (!item.empty()) {
        segs.emplace_back(item);
      }
    }

    // We don't expect strings with the "../" prefix
    assert(segs[0] != "..");

    // We have a filename only if we don't have a terminator
    has_terminator_ = *(--src_path_.end()) == '/';
    if (!has_terminator_) {
      filename_ = *(--segs.end());
    }

    //函数会将所有的段重新组合成一个字符串，作为规范化的路径。在这个过程中，如果源路径是一个目录，那么父路径就是规范化的路径
    // Keep the root slash
    if (src_path_[0] == '/') {
      rst << "/";
    }

    for (size_t i = 0; i < segs.size(); ++i) {
      rst << segs[i];
      // The parent path represent higher-level directory, note that if
      // the source path is already a directory, the result reminds the
      // same.
      //
      // `/a/b/c   ->  /a/b/ `
      // `/a/b/c/` ->  /a/b/c/ `
      if (!has_terminator_ && i == segs.size() - 2) {
        parent_path_ = rst.str() + "/";
      }
      if (!has_terminator_ && i == segs.size() - 1) {
        break;
      }
      rst << "/";
    }

    normalized_path_ = rst.str();
    // If current source path is a directory, the parent path reminds the
    // same (@see std::filesystem::path::parent_path())
    if (has_terminator_) {
      parent_path_ = normalized_path_;
    }
    return *this;
  }

  std::string parent_path() const { return parent_path_; }

  path filename() { return path(filename_); }

  bool has_filename() { return !filename_.empty(); }

  path operator/(const path& other) const {
    std::string seperator = "/";
    if (has_terminator_) {
      seperator = "";
    }
    return path(normalized_path_ + seperator + other.string());
  }

 private:
  std::string src_path_;       //源文件路径
  std::string normalized_path_;//标准化路径
  std::string parent_path_;    //父目录路径
  // @see std::filesystem::path::filename()
  std::string filename_;       //文件名
  bool has_terminator_ = false;//是否以路径分隔符结束
};
}  // namespace filesystem_utility
