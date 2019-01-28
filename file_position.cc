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

#include "file_position.h"

#include "absl/strings/numbers.h"

namespace mysql_ripple {

bool FilePosition::Parse(absl::string_view sv) {
  if (sv.size() <= 1) {
    return false;
  }
  if (sv.front() == '\'' && sv.back() == '\'') {
    sv = sv.substr(1, sv.size() - 2);
  } else {
    if (sv.front() == '\'' || sv.back() == '\'') {
      return false;
    }
  }

  auto pos = sv.find(':');
  if (pos == 0 || pos == absl::string_view::npos) {
    return false;
  }

  filename = std::string(sv.substr(0, pos));

  return absl::SimpleAtoi(sv.substr(pos + 1), &offset);
}

std::string FilePosition::ToString() const {
  return filename + ":" + std::to_string(offset);
}

}  // namespace mysql_ripple
