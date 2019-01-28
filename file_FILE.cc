// Copyright 2018 The Ripple Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "file_FILE.h"

#include <sys/stat.h>
#include <unistd.h>

namespace {

using mysql_ripple::Buffer;
using mysql_ripple::file::AppendOnlyFile;
using mysql_ripple::file::Factory;
using mysql_ripple::file::InputFile;

class FI : public InputFile, public AppendOnlyFile {
 public:
  explicit FI(FILE *file) : file_(file) {}
  ~FI() {}

  // close file.
  bool Close() override {
    auto res = fclose(file_);
    file_ = nullptr;
    delete this;
    return res == 0;
  }

  // return current file position into offset.
  bool Tell(int64_t *offset) override {
    auto res = ftell(file_);
    if (res == -1) return false;
    *offset = res;
    return true;
  }

  // set current file position to offset.
  bool Seek(int64_t offset) override {
    return fseek(file_, offset, SEEK_SET) == 0;
  }

  // truncate file to new_size.
  bool Truncate(int64_t new_size) override {
    return ftruncate(fileno(file_), new_size) == 0;
  }

  // read size bytes from current file position, appending into buffer.
  bool Read(Buffer &b, int64_t size) override {
    auto end = b.Append(size) + size;
    while (size > 0) {
      size -= fread(end - size, 1, size, file_);
      if (ferror(file_) && errno == EINTR) continue;
      if (size == 0) break;
      b.resize(b.size() - size);
      return false;
    }
    return true;
  }

  // write size bytes from buffer to file at current file position.
  bool Write(absl::string_view data) override {
    return fwrite(data.data(), 1, data.size(), file_) == data.size();
  }

  // flush pending writes.
  bool Flush() override { return fflush(file_) == 0; }

  // make writes durable.
  bool Sync() override { return Flush() && (fsync(fileno(file_)) == 0); }

  bool eof() override { return feof(file_); }

 private:
  FILE *file_;
};

class FF : public Factory {
  bool Create(AppendOnlyFile **file, absl::string_view filename,
              absl::string_view mode) const override {
    std::string name(filename);
    FILE *check = fopen(name.c_str(), "r");
    if (check != nullptr) {
      fclose(check);
      return false;
    }
    return Open(file, filename, mode);
  }

  bool Open(AppendOnlyFile **file, absl::string_view filename,
            absl::string_view mode) const override {
    std::string name(filename), mode_str(mode);
    FILE *f = fopen(name.c_str(), mode_str.c_str());
    if (f == nullptr) return false;
    *file = new FI(f);
    return true;
  }

  bool Open(InputFile **file, absl::string_view filename,
            absl::string_view mode) const override {
    std::string name(filename), mode_str(mode);
    FILE *f = fopen(name.c_str(), mode_str.c_str());
    if (f == nullptr) return false;
    *file = new FI(f);
    return true;
  }

  bool Delete(absl::string_view filename) const override {
    std::string name(filename);
    return unlink(name.c_str()) == 0;
  }

  bool Rename(absl::string_view filename,
              absl::string_view newname) const override {
    std::string name(filename), newname_str(newname);
    return rename(name.c_str(), newname_str.c_str()) == 0;
  }

  bool Finalize(absl::string_view filename) const override {
    // Finalize does nothing in this implementation.
    return true;
  }

  bool Archive(absl::string_view filename) const override {
    // Archive does nothing in this implementation.
    return true;
  }

  bool Size(absl::string_view filename, int64_t *size) const override {
    struct stat st;
    std::string name(filename);
    if (stat(name.c_str(), &st) != 0) return false;
    *size = st.st_size;
    return true;
  }

  bool Mtime(absl::string_view filename, absl::Time *time) const override {
    struct stat st;
    std::string name(filename);
    if (stat(name.c_str(), &st) != 0) return false;
    *time = absl::FromTimeT(st.st_mtime);
    return true;
  }
};

const FF theFactory;

}  // namespace

namespace mysql_ripple {

namespace file {

const Factory &FILE_Factory() { return theFactory; }

}  // namespace file

}  // namespace mysql_ripple
