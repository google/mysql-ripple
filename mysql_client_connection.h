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

#ifndef MYSQL_RIPPLE_MYSQL_CLIENT_CONNECTION_H
#define MYSQL_RIPPLE_MYSQL_CLIENT_CONNECTION_H

#include <string>
#include <memory>

#include "buffer.h"
#include "connection.h"
#include "gtid.h"

// Forward declare MYSQL object to avoid including include/mysql.h
// which pulls in "all" of mysql.
typedef struct st_mysql MYSQL;

namespace mysql_ripple {

namespace mysql {

// A class representing a connection from ripple to a mysql server.
class ClientConnection : public Connection {
 public:
  ClientConnection();
  virtual ~ClientConnection();

  // Enable/disable compressed
  // Shall be used *before* Connect()
  virtual void SetCompressed(bool val) {
    compress_ = val;
  }

  // Set heartbeat period used for replication stream.
  // Shall be used *before* Connect().
  virtual void SetHeartbeatPeriod(double heartbeat_period_seconds) {
    heartbeat_period_ = heartbeat_period_seconds;
  }

  // Connect to server.
  // This method shall be called if connection status is DISCONNECTED or
  // CONNECT_FAILED otherwise it will return false directly.
  // This method blocks and return true once connection established and
  // server variant and version is checked.
  virtual bool Connect(const char *name,
                       const char *host,
                       int port,
                       const char *protocol,
                       const char *user,
                       const char *password);

  // Read a packet.
  // The returned packet is valid until next call.
  // On error a packet with len = -1 is returned.
  // This method is blocking.
  virtual Packet ReadPacket();

  // Disconnect.
  // This method blocks.
  void Disconnect() override;

  // Abort a connection.
  // This method aborts a connection that is CONNECTING or CONNECTED.
  // This method is non blocking and returns true if the connection was
  // in correct state and false otherwise. The "socket implementation" of this
  // is shutdown(2).
  void Abort() override;

  struct ServerVersion {
    int major_version;
    int minor_version;
    int patch_level;
    std::string comment;
  };

  enum MysqlVariant {
    MYSQL_VARIANT_UNKNOWN,  //
    MYSQL_VARIANT_MARIADB,  // MariaDB
    MYSQL_VARIANT_MYSQL     // MySQL (e.g WebScaleSQL or Oracle)
  };

  // Get server version from a connected mysqld.
  // Return cached version or calls FetchServerVersion().
  virtual ServerVersion GetServerVersion();
  virtual bool FetchServerVersion();

  // Get server id from a connected mysqld.
  // Return cached version or calls FetchServerId().
  virtual ServerId GetServerId();
  virtual bool FetchServerId();

  // Fetch a variable from connected mysqld.
  // SHOW VARIABLE LIKE 'var'
  // return true on success.
  virtual bool FetchVariable(const std::string& variable,
                             std::string *result_value);

  // Check if connected mysqld supports being a semi-sync master
  // and if it has semi-sync enabled.
  // return false on error
  // return true otherwise (and updates supported/enabled)
  virtual bool CheckSupportsSemiSync(bool *supported, bool *enabled);

  // Returns cached version (always)
  virtual ServerId GetServerId() const { return server_id_; }

  // Returns cached version (always)
  virtual ServerVersion GetServerVersion() const { return server_version_; }

  // Start replication stream.
  bool StartReplicationStream(const GTIDList&, bool semi_sync);

  // Write a packet.
  // This method is blocking.
  virtual bool WritePacket(Packet packet);

  bool WritePacket(const Buffer& buffer) {
    Packet p = { static_cast<int>(buffer.size()), buffer.data() };
    return WritePacket(p);
  }

  // Reset packet number
  virtual void Reset();

  // These methods are for monitoring
  std::string GetHost() const override { return host_; }
  std::string GetUser() const { return user_; }
  std::string GetPassword() const { return password_; }
  uint16_t GetPort() const override { return port_; }
  std::string GetProtocol() const { return protocol_; }

  bool ExecuteStatement(const std::string& query);

 private:
  std::unique_ptr<MYSQL> mysql_;
  MysqlVariant mysql_variant_;
  ServerVersion server_version_;
  ServerId server_id_;
  bool compress_;
  double heartbeat_period_;

  // saved for monitoring and error messages
  std::string connection_name_;
  std::string host_;
  uint16_t port_;
  std::string protocol_;
  std::string user_;
  std::string password_;

  bool FetchMysqlVariant();
  bool ExecuteSetHeartbeatPeriod(double seconds);

  // Start replication for either mysql or mariadb
  bool StartReplicationStreamMysql(const GTIDList&, bool semi_sync);
  bool StartReplicationStreamMariaDB(const GTIDList&, bool semi_sync);

  // Parse protocol string (e.g tcp) and return value suitable for
  // mysql_options(MYSQL_OPT_PROTOCOL).
  unsigned ParseProtocolString(const char *str);

  ClientConnection(ClientConnection&&) = delete;
  ClientConnection(const ClientConnection&) = delete;
  Connection& operator=(ClientConnection&&) = delete;
  Connection& operator=(const ClientConnection&) = delete;
};

}  // namespace mysql

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_CLIENT_CONNECTION_H
