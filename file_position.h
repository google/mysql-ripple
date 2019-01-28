/*
 * Copyright 2018 The Ripple Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MYSQL_RIPPLE_FILE_POSITION_H
#define MYSQL_RIPPLE_FILE_POSITION_H

#include <string>

#include "absl/time/time.h"
#include "absl/strings/string_view.h"

namespace mysql_ripple {

// This class represents a file position.
struct FilePosition {
  FilePosition() : offset(0) {}
  ~FilePosition() {}
  FilePosition(const std::string& name, off_t off)
      : filename(name), offset(off) {}
  explicit FilePosition(const std::string& name)
      : filename(name), offset(0) {}

  std::string filename;
  off_t offset;

  void Init(const std::string& name) {
    filename = name;
    offset = 0;
  }

  // Reset position.
  void Reset() { filename.clear(); offset = 0;}
  bool IsEmpty() const { return filename.size() == 0 && offset == 0; }

  bool equal(const FilePosition& other) const {
    if (filename.compare(other.filename))
      return false;
    if (offset != other.offset)
      return false;
    return true;
  }

  bool Parse(absl::string_view sv);
  std::string ToString() const;
};

// This is used to expose binlog end position.
// It's used by BinlogReader, and is implemented
// by the Binlog class. There is an additional
// implementation used during Recover().
class BinlogEndPositionProviderInterface {
 public:
  virtual ~BinlogEndPositionProviderInterface() {}

  // Wait/Get for a file position other than pos (for BinlogReader).
  //
  // truncate_counter - OUT return no of times binlog has been truncated.
  //
  // return false - if position is equal even after timeout
  //        true  - if position is different (and in that case store new
  //                position in pos)
  // Thread safe.
  virtual bool WaitBinlogEndPosition(FilePosition* pos,
                                     int64_t* truncate_counter,
                                     absl::Duration timeout) = 0;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_FILE_POSITION_H
