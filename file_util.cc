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

#include "file_util.h"

#include "absl/strings/string_view.h"
#include "buffer.h"
#include "file.h"
#include "logging.h"

namespace mysql_ripple {

namespace file_util {

OpenResultCode OpenAndValidate(file::InputFile **file, const file::Factory &ff,
                               absl::string_view filename,
                               absl::string_view mode,
                               absl::string_view header) {
  file::InputFile *f;
  if (!ff.Open(&f, filename, mode)) {
    *file = nullptr;
    return NO_SUCH_FILE;
  }
  Buffer buf;
  f->Read(buf, header.size());
  if (buf.empty()) {
    f->Close();
    *file = nullptr;
    return FILE_EMPTY;
  }
  if (header != (absl::string_view)(buf)) {
    f->Close();
    *file = nullptr;
    LOG(ERROR) << "Invalid magic. bytes read: " << buf.size();
    return INVALID_MAGIC;
  }
  *file = f;
  return OK;
}

}  // namespace file_util

}  // namespace mysql_ripple
