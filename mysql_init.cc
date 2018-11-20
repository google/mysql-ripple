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

#include "mysql_init.h"

#include <cstdlib>  // nullptr

#include "mysql.h"

namespace mysql_ripple {

namespace mysql {

// Initialize mysql client library.
// This method is not thread safe and shall only be called once.
bool InitClientLibrary() {
  if (mysql_library_init(0, nullptr, nullptr)) {
    return false;
  }
  return true;
}

void DeinitClientLibrary() {
  mysql_library_end();
}

bool ThreadInit() {
  if (my_thread_init()) {
    return false;
  }

  return true;
}

void ThreadDeinit() {
  my_thread_end();
}

}  // namespace mysql

}  // namespace mysql_ripple
