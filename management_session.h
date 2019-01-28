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

#ifndef MYSQL_RIPPLE_MANAGEMENT_SESSION_H
#define MYSQL_RIPPLE_MANAGEMENT_SESSION_H

#include "manager.h"
#include "mysql_server_port.h"
#include "session.h"

namespace mysql_ripple {

class ManagementSession : public ThreadedSession {
 public:
  explicit ManagementSession(Manager::RippledInterface* rippled)
      : ThreadedSession(Session::MgmSession), manager_(new Manager(rippled)) {}

  // Method return value indicates whether port sharing was a success.
  virtual bool ShareServerPort(mysql::ServerPort *port) { return false; }
  virtual void Shutdown(mysql::ServerPort *port) { }

 protected:
  std::unique_ptr<Manager> manager_;
};

class ManagementSessionFactory {
 public:
  // Create a Management Session
  static ManagementSession* GetInstance(Manager::RippledInterface* rippled,
                                        const std::string& type);
  virtual ~ManagementSessionFactory() {}

 protected:
  virtual ManagementSession* NewInstance(
      Manager::RippledInterface* rippled) = 0;

  virtual bool Match(const std::string& type) = 0;

  static bool Register(ManagementSessionFactory *factory);
};

};  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MANAGEMENT_SESSION_H
