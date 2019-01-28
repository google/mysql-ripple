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

#include "mysql_server_port.h"

#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "logging.h"

namespace mysql_ripple {

namespace mysql {

static const int MAX_SERVICES = 10;

// "global" repository of server ports
static ServerPortFactory *serverPorts[MAX_SERVICES] = { nullptr };

ServerPortFactory::~ServerPortFactory() {
}

bool ServerPortFactory::Register(ServerPortFactory *factory) {
  for (int i = 0; i < MAX_SERVICES; i++) {
    if (serverPorts[i] == nullptr) {
      serverPorts[i] = factory;
      return true;
    }
  }
  LOG(ERROR) << "Failed to register server port factory, no free slot"
             << ", arg: " << static_cast<const void*>(factory);
  return false;
}

ServerPort* ServerPortFactory::GetInstance(const std::string& type,
                                           const std::string& address,
                                           const std::string& ports) {
  int failed = 0;
  ServerPortFactory *factory = nullptr;
  if (type.empty()) {
    // use first type registered
    factory = serverPorts[0];
  } else {
    for (auto serverPort : serverPorts) {
      if (serverPort == nullptr)
        continue;
      if (serverPort->Match(type)) {
        factory = serverPort;
        break;
      }
      failed++;
    }
  }

  if (factory == nullptr) {
    LOG(ERROR) << "Failed to create ServerPort instance"
               << ", no match"
               << ", type: " << type
               << ", address: " << address
               << ", examined: " << failed;
    return nullptr;
  }

  // parse ports-string into array of int
  std::vector<int> port_list;
  for (auto s : absl::StrSplit(ports, ',', absl::SkipWhitespace())) {
    int i;
    if (absl::SimpleAtoi(s, &i)) {
      port_list.push_back(i);
    } else {
      LOG(WARNING) << "Ignoring port: \"" << s << "\"";
    }
  }

  if (port_list.empty()) {
    LOG(FATAL) << "Found 0 ports! arg: \"" << ports << "\"";
  }

  return factory->NewInstance(address, port_list);
}

}  // namespace mysql

}  // namespace mysql_ripple
