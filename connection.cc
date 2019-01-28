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

#include "connection.h"

#include <stdio.h>

namespace mysql_ripple {

Connection::Connection(ConnectionType type)
    : connection_status_(DISCONNECTED), connection_type_(type), bytes_sent(0),
      bytes_received(0) {
}

Connection::~Connection() {
}

// Check if there is a latest error.
bool Connection::HasError() const {
  return !last_error_message_.empty();
}

// Get last error message.
// This message is reset before each method called on this class
// except HasError and GetLastErrorMessage.
std::string Connection::GetLastErrorMessage() const {
  return last_error_message_;
}

void Connection::SetError(const std::string &msg) const {
  last_error_message_.assign(msg);
}

}  // namespace mysql_ripple
