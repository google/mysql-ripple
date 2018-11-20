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

#include "mysql_server_connection.h"

#include <netinet/in.h>
#include <cstdio>
#include <utility>

#include "monitoring.h"

// MySQL client library includes
#include "mysql.h"
#include "mysqld_error.h"
// Vio
#include "private/violite.h"


namespace mysql_ripple {

namespace mysql {

ServerConnection::ServerConnection(MYSQL *mysql, std::string address,
                                   uint16 port)
    : Connection(MYSQL_SERVER_CONNECTION),
      mysql_(mysql),
      host_(std::move(address)),
      port_(port) {}

ServerConnection::~ServerConnection() {
  Disconnect();
  if (mysql_ != nullptr)  {
    net_end(&mysql_->net);
    vio_delete(mysql_->net.vio);
    mysql_->net.vio = 0;
    mysql_close(mysql_.release());
  }
}

void ServerConnection::Disconnect() {
  // TODO(pivanof): Need opensource version of force-disconnect.
}

void ServerConnection::Abort() {
}

// Read a packet.
// The returned packet is valid until next call.
// On error a packet with len = -1 is returned.
// This method is blocking.
ServerConnection::Packet ServerConnection::ReadPacket() {
  ulong len = my_net_read(&mysql_->net);
  if (len == packet_error) {
    Packet p = { -1, nullptr };
    return p;
  }

  bytes_received += len;

  Packet p = { static_cast<int>(len), mysql_->net.read_pos };
  return p;
}

// Write a packet.
// This method is blocking.
bool ServerConnection::WritePacket(Packet packet) {
  /*
    printf("WritePacket nr: %d data(%d): ", mysql_->net.pkt_nr, packet.length);
    for (int i = 0; i < packet.length; i++)
      printf("%.2x ", packet.ptr[i]);
    printf("\n");
  */

  if (my_net_write(&mysql_->net, packet.ptr, packet.length)) {
    SetError("Failed to write packet");
    return false;
  }

  bytes_sent += packet.length;

  if (net_flush(&mysql_->net)) {
    SetError("Failed net_flush after my_net_write");
    return false;
  }

  return true;
}

void ServerConnection::Reset() {
  mysql_->net.pkt_nr = 0;
}

// Accept a connection.
// If return true the connection takes ownership of vio.
// If return false the caller keeps ownership of vio.
// Note that only ReadPacket/WritePacket may be used on this connection.
ServerConnection* ServerConnection::Accept(Vio *vio) {
  MYSQL *mysql = mysql_init(0);

  int flags = 0;
  if (my_net_init(&mysql->net, vio, NULL, flags)) {
    mysql->net.vio = nullptr;
    mysql_close(mysql);
    return nullptr;
  }

  char address[INET6_ADDRSTRLEN];
  uint16_t port;
  vio_peer_addr(vio, address, &port, sizeof(address));

  ServerConnection *con = new ServerConnection(mysql, std::string(address),
                                               port);
  if (con == nullptr) {
    mysql_close(mysql);
  }
  return con;
}

void ServerConnection::SetCompressed(bool val) {
  if (val)
    mysql_->net.compress = 1;
  else
    mysql_->net.compress = 0;
}


}  // namespace mysql

}  // namespace mysql_ripple
