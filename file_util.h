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

#ifndef MYSQL_RIPPLE_FILE_UTIL_H
#define MYSQL_RIPPLE_FILE_UTIL_H

#include "absl/strings/string_view.h"
#include "file.h"

namespace mysql_ripple {

namespace file_util {

enum OpenResultCode {
  NO_SUCH_FILE,
  FILE_EMPTY,
  INVALID_MAGIC,
  OK
};

OpenResultCode OpenAndValidate(file::InputFile** file, const file::Factory& ff,
                               absl::string_view filename,
                               absl::string_view mode,
                               absl::string_view header);

enum ReadResultCode {
  READ_OK,    // Read call was OK
  READ_EOF,   // Read reach end of file
  READ_ERROR  // Other read error
};

}  // namespace file_util

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_FILE_UTIL_H
