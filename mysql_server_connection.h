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

#ifndef MYSQL_RIPPLE_MYSQL_SERVER_CONNECTION_H
#define MYSQL_RIPPLE_MYSQL_SERVER_CONNECTION_H

#include <string>
#include <memory>

#include "buffer.h"
#include "connection.h"

// Forward declare MYSQL object to avoid including include/mysql.h
// which pulls in "all" of mysql.
typedef struct st_mysql MYSQL;

// Forward declare Vio to avoid including include/mysql.h
// which pulls in "all" of mysql.
typedef struct st_vio Vio;

namespace mysql_ripple {

namespace mysql {

// A class representing a mysql connection to ripple
class ServerConnection : public Connection {
 public:
  virtual ~ServerConnection();

  // Disconnect.
  // This method blocks.
  void Disconnect() override;

  // Abort a connection.
  // This method aborts a connection that is CONNECTING or CONNECTED.
  // This method is non blocking and returns true if the connection was
  // in correct state and false otherwise. The "socket implementation" of this
  // is shutdown(2).
  void Abort() override;

  // Read a packet.
  // The returned packet is valid until next call.
  // On error a packet with len = -1 is returned.
  // This method is blocking.
  virtual Packet ReadPacket();

  // Write a packet.
  // This method is blocking.
  virtual bool WritePacket(Packet packet);

  virtual bool WritePacket(const Buffer& packet) {
    Packet p = { static_cast<int>(packet.size()), packet.data() };
    return WritePacket(p);
  }

  // Reset packet number
  virtual void Reset();

  // Accept a connection.
  static ServerConnection* Accept(Vio *vio);

  // Enabled/disable compressed protocol
  // (default is disabled).
  virtual void SetCompressed(bool val);

  std::string GetHost() const override { return host_; }
  uint16_t GetPort() const override { return port_; }

 protected:
  std::unique_ptr<MYSQL> mysql_;

 private:
  // Constructor for Accept
  ServerConnection(MYSQL *mysql, std::string address, uint16_t port);
  const std::string host_;
  const uint16_t port_;
};

}  // namespace mysql

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_SERVER_CONNECTION_H
