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

#ifndef MYSQL_RIPPLE_MYSQL_SERVER_PORT_TCPIP_H
#define MYSQL_RIPPLE_MYSQL_SERVER_PORT_TCPIP_H

#include <string>

#include "mysql_server_port.h"

namespace mysql_ripple {

namespace mysql {

class TcpIpServerPort : public ServerPort {
 public:
  TcpIpServerPort(const std::string& address,
                  const std::vector<int>& ports);
  virtual ~TcpIpServerPort();

  bool Bind() override;
  bool Listen() override;
  // return unauthenticated RawConnection
  ServerConnection* Accept() override;

  // Shutdown (wakes up thread calling Accept())
  bool Shutdown() override;

  // Close server port
  bool Close() override;

 private:
  std::string address_;
  int port_;
  int socket_;
};

class TcpIpServerPortFactory : public ServerPortFactory {
 public:
  virtual ~TcpIpServerPortFactory() {}

  ServerPort *NewInstance(const std::string& address,
                          const std::vector<int>& ports) override {
    return new TcpIpServerPort(address, ports);
  }

  bool Match(const std::string& type) override {
    return type.compare("tcp") == 0;
  }

  static bool Register() {
    return ServerPortFactory::Register(new TcpIpServerPortFactory());
  }

 private:
  TcpIpServerPortFactory() {}
};

}  // namespace mysql

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_SERVER_PORT_TCPIP_H
