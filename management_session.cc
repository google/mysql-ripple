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

#include "management_session.h"

#include "logging.h"

namespace mysql_ripple {

static const int MAX_SERVICES = 10;

static ManagementSessionFactory *managementFactories[MAX_SERVICES] =
    { nullptr };

bool ManagementSessionFactory::Register(
  ManagementSessionFactory *factory) {
  for (int i = 0; i < MAX_SERVICES; i++) {
    if (managementFactories[i] == nullptr) {
      managementFactories[i] = factory;
      return true;
    }
  }

  LOG(ERROR) << "Failed to register management session factory, no free slot";
  return false;
}

ManagementSession *ManagementSessionFactory::GetInstance(
    Manager::RippledInterface *rippled, const std::string &type) {
  int failed = 0;
  ManagementSessionFactory *factory = nullptr;
  if (type.empty()) {
    // use first management session type registered
    factory = managementFactories[0];
  } else {
    for (auto mgmFactory : managementFactories) {
      if (mgmFactory != nullptr && mgmFactory->Match(type)) {
        factory = mgmFactory;
        break;
      }
      failed++;
    }
  }
  if (factory == nullptr) {
    LOG(ERROR) << "Failed to create ManagementSession instance"
               << ", no match for type: " << type
               << ", examined: " << failed;
    return nullptr;
  }
  return factory->NewInstance(rippled);
}

}  // namespace mysql_ripple
