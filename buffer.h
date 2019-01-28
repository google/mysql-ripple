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

#ifndef MYSQL_RIPPLE_BUFFER_H
#define MYSQL_RIPPLE_BUFFER_H

#include <stdlib.h>

#include <cstdint>
#include <vector>

#include "absl/strings/string_view.h"

namespace mysql_ripple {

class Buffer : public std::vector<uint8_t> {
 public:
  // Extend size by n bytes and return pointer to
  // start place of extension.
  uint8_t* Append(size_t n) {
    size_t curr = size();
    resize(curr + n);
    return data() + curr;
  }

  // Append n bytes from ptr to end of this buffer.
  bool Append(const uint8_t *ptr, size_t n) {
    insert(end(), ptr, ptr + n);
    return true;
  }

  // Implicit conversion to absl::string_view.
  operator absl::string_view() const {
    auto b = reinterpret_cast<const char*>(data());
    return absl::string_view(b, size());
  }
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_BUFFER_H
