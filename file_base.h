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

#ifndef MYSQL_RIPPLE_FILE_BASE_H
#define MYSQL_RIPPLE_FILE_BASE_H

#include <stdlib.h>
#include <cstdint>

#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "buffer.h"

namespace mysql_ripple {

namespace file {

class BaseFile {
 public:
  // close file.
  // this method does delete *this hence this is unusable after this call.
  virtual bool Close() = 0;

  // return current file position into offset.
  virtual bool Tell(int64_t *offset) = 0;

 protected:
  virtual ~BaseFile() {}
};

class InputFile : public virtual BaseFile {
 public:
  // set current file position to offset.
  virtual bool Seek(int64_t offset) = 0;

  // read size bytes from current file position, appending into b.
  virtual bool Read(Buffer &b, int64_t size) = 0;

  // return true if the current position is at end of file.
  virtual bool eof() = 0;
};

class AppendOnlyFile : public virtual BaseFile {
 public:
  // truncate file to size.
  virtual bool Truncate(int64_t size) = 0;

  // write data to file at current file position.
  virtual bool Write(const absl::string_view data) = 0;

  // flush pending writes.
  virtual bool Flush() = 0;

  // make writes durable.
  virtual bool Sync() = 0;
};

class Factory {
 public:
  virtual bool Create(AppendOnlyFile **file, absl::string_view filename,
                      absl::string_view mode) const = 0;

  virtual bool Open(AppendOnlyFile **file, absl::string_view filename,
                    absl::string_view mode) const = 0;

  virtual bool Open(InputFile **file, absl::string_view filename,
                    absl::string_view mode) const = 0;

  virtual bool Delete(absl::string_view filename) const = 0;

  virtual bool Rename(absl::string_view filename,
                      absl::string_view newname) const = 0;

  virtual bool Finalize(absl::string_view filename) const = 0;

  virtual bool Archive(absl::string_view filename) const = 0;

  virtual bool Size(absl::string_view filename, int64_t *size) const = 0;

  virtual bool Mtime(absl::string_view filename, absl::Time *time) const = 0;

 protected:
  Factory() {}
  virtual ~Factory() {}
};

}  // namespace file

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_FILE_BASE_H
