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

#include "listener.h"

#include "mysql_init.h"

namespace mysql_ripple {

Listener::Listener(SessionFactory *factory,
                   mysql::ServerPort *port)
    : ThreadedSession(Session::Listener),
      factory_(factory), port_(port) {
}

Listener::~Listener() {
}

bool Listener::Stop() {
  if (ThreadedSession::Stop()) {
    port_->Shutdown();
    return true;
  }
  return false;
}

void* Listener::Run() {
  mysql::ThreadInit();
  while (!ShouldStop()) {
    Connection *con = port_->Accept();
    if (con != nullptr) {
      if (!factory_->NewSession(con)) {
        delete con;
      }
    }
  }
  mysql::ThreadDeinit();
  return nullptr;
}

}  // namespace mysql_ripple
