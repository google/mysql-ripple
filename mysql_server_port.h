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

#ifndef MYSQL_RIPPLE_MYSQL_SERVER_PORT_H
#define MYSQL_RIPPLE_MYSQL_SERVER_PORT_H

#include <string>

#include "mysql_server_connection.h"

namespace mysql_ripple {

namespace mysql {

// A class representing a mysql server port.
class ServerPort {
 public:
  ServerPort() {}
  virtual ~ServerPort() {}

  virtual bool Bind() = 0;
  virtual bool Listen() = 0;
  // return unauthenticated ServerConnection
  virtual ServerConnection* Accept() = 0;

  // Shutdown (wakes up thread calling Accept())
  virtual bool Shutdown() = 0;

  // Close server port
  virtual bool Close() = 0;
};

class ServerPortFactory {
 public:
  // Shall be freed by caller.
  static ServerPort* GetInstance(const std::string& type,
                                 const std::string& address,
                                 const std::string& ports);
 protected:
  virtual ~ServerPortFactory();

  // Create a ServerPort.
  // Used by GetInstance()
  // Shall be freed by caller.
  virtual ServerPort* NewInstance(const std::string& address,
                                  const std::vector<int>& ports) = 0;

  // Can this ServerPortFactory provide ports of requested type
  virtual bool Match(const std::string& type) = 0;

  // Register a ServerPortFactory.
  static bool Register(ServerPortFactory *factory);
};

}  // namespace mysql

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_SERVER_PORT_H
