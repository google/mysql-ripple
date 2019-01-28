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

#include "mysql_server_port_tcpip.h"

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "logging.h"
#include "mysql_constants.h"
#include "plugin.h"

#include "private/violite.h"

namespace mysql_ripple {

namespace plugin {

DECLARE_PLUGIN(mysql_server_port_tcpip) {
  return mysql::TcpIpServerPortFactory::Register();
}

}  // namespace plugin

namespace mysql {

TcpIpServerPort::TcpIpServerPort(const std::string& address,
                                 const std::vector<int>& ports)
    : address_(address), socket_(-1) {

  port_ = ports[0];
  if (ports.size() > 1) {
    //TODO (jonaso) :
    LOG(WARNING) << "TcpIpServerPort only supports 1 port."
                 << " Using " << port_ << " and ignoring rest ("
                 << (ports.size() - 1) << " ports)";
  }
}

TcpIpServerPort::~TcpIpServerPort() {
  Close();
}

static bool Resolve(const std::string& address, struct sockaddr_in *dst) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  struct addrinfo* ai_list;
  int res = getaddrinfo(address.c_str(), nullptr, &hints, &ai_list);
  if (res != 0) {
    LOG(ERROR) << "Failed to lookup '" << address << "'"
               << ", res: " << res;
    return false;
  }

  // return first in list
  struct sockaddr_in* sin = (struct sockaddr_in*)ai_list->ai_addr;
  memcpy(&dst->sin_addr, &sin->sin_addr, sizeof(struct in_addr));

  freeaddrinfo(ai_list);
  return true;
}

bool TcpIpServerPort::Bind() {
  int fd = -1;

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  const char *nodename= NULL;
  if (address_.length() == 0) {
    hints.ai_flags = AI_PASSIVE;
  } else {
    nodename = address_.c_str();
  }

  struct addrinfo* ai_list;
  int res = getaddrinfo(nodename, nullptr, &hints, &ai_list);
  if (res != 0) {
    PLOG(ERROR) << "Failed to lookup '" << address_ << "'"
               << ", res: " << res;
    return false;
  }

  struct addrinfo* ai= NULL;
  // Loop through all possible addresses.
  for (ai = ai_list; ai != NULL; ai = ai->ai_next) {
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd == -1) {
      PLOG(INFO)
          << "Failed to create socket with "
          << "domain: " << ai->ai_family
          << "type: " << ai->ai_socktype
          << "protocol: " << ai->ai_protocol;
      continue;
    }
    const int on = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
      close(fd);
      continue;
    }
    if (ai->ai_family == AF_INET) {
      reinterpret_cast<struct sockaddr_in*>(ai->ai_addr)->sin_port
          = htons(port_);
    } else if (ai->ai_family == AF_INET6) {
      reinterpret_cast<struct sockaddr_in6*>(ai->ai_addr)->sin6_port
          = htons(port_);
    } else {
      LOG(INFO) << "Unknown family!" << ai->ai_family;
      close(fd);
      continue;
    }
    if (bind(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
      // success
      break;
    }
    PLOG(INFO) << "Failed to bind";
    close(fd);
  }
  freeaddrinfo(ai_list);
  if (ai == NULL) {
    LOG(ERROR) << "Unable to create server port for '"
               << address_ << "' port: " << port_;
    return false;
  }

  if (address_.length() == 0) {
    LOG(INFO) << "Listen on host: * "
              << ", port: " << port_;
  } else {
    LOG(INFO) << "Listen on host: " << address_
              << ", port: " << port_;
  }
  socket_ = fd;
  return true;
}

bool TcpIpServerPort::Listen() {
  if (socket_ == -1)
    return false;

  if (listen(socket_, 32) != 0) {
    PLOG(ERROR) << "listen() failed";
    return false;
  }

  return true;
}

bool TcpIpServerPort::Shutdown() {
  if (socket_ == -1)
    return false;
  shutdown(socket_, SHUT_RDWR);
  return true;
}

ServerConnection* TcpIpServerPort::Accept() {
  if (socket_ == -1) {
    return nullptr;
  }

  int new_sock = accept(socket_, 0, 0);

  if (new_sock == -1) {
    PLOG(ERROR) << "accept() failed";
    return nullptr;
  }

  Vio *new_vio = nullptr;
  ServerConnection *new_connection = nullptr;
  do {
    int flags = 0;
    MYSQL_SOCKET mysql_socket = { new_sock };
    new_vio = mysql_socket_vio_new(mysql_socket, VIO_TYPE_TCPIP, flags);
    if (new_vio == nullptr)
      break;

    new_connection = ServerConnection::Accept(new_vio);
    if (new_connection == nullptr)
      break;

    return new_connection;
  } while (0);

  if (new_connection != nullptr)
    delete new_connection;

  if (new_vio != nullptr)
    vio_delete(new_vio);

  if (new_sock != -1)
    close(new_sock);

  return nullptr;
}

bool TcpIpServerPort::Close() {
  if (socket_ != -1) {
    close(socket_);
    socket_ = -1;
  }
  return true;
}

}  // namespace mysql

}  // namespace mysql_ripple
