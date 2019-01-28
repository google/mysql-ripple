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

#include "mysql_client_connection.h"

#include <stdio.h>

#include "absl/strings/numbers.h"
#include "byte_order.h"
#include "flags.h"
#include "logging.h"
#include "monitoring.h"
#include "mysql_compat.h"
#include "mysql_constants.h"
#include "mysql_protocol.h"
#include "mysql_util.h"

// MySQL client library includes
#include "mysql.h"
#include "mysqld_error.h"


// This are functions inside mysql client library that are not exported.
extern "C" {
ulong cli_safe_read(MYSQL *mysql);
my_bool cli_advanced_command(MYSQL *mysql,
                             enum enum_server_command command,
                             const unsigned char *header, ulong header_length,
                             const unsigned char *arg, ulong arg_length,
                             my_bool skip_check, MYSQL_STMT *stmt);

#define simple_command(mysql, command, arg, length, skip_check) \
  cli_advanced_command(mysql, command, 0, 0, arg, length, skip_check, nullptr)
};

namespace mysql_ripple {

namespace mysql {

// This start position is "default"
// and points to first event after binlog file header.
// However it's not really used when using GTIDs, but
// we send it anyway...as all other mysql versions does so.
static int kStartPos = 4;

ClientConnection::ClientConnection() :
    Connection(MYSQL_CLIENT_CONNECTION),
    mysql_variant_(MYSQL_VARIANT_UNKNOWN),
    compress_(false),
    heartbeat_period_(0.1) {
  mysql_.reset(mysql_init(0));
  Disconnect();
}

ClientConnection::~ClientConnection() {
  Disconnect();
  if (mysql_ != nullptr)  {
    mysql_close(mysql_.release());
  }
}

// Connect to server.
// This method shall be called if connection status is DISCONNECTED or
// CONNECT_FAILED otherwise it will return false directly.
// This method blocks and return true once connection established and
// server variant and version is checked.
bool
ClientConnection::Connect(const char *name,
                          const char *host,
                          int port,
                          const char *protocol,
                          const char *user) {
  ClearError();

  {
    absl::MutexLock lock(&mutex_);
    if (connection_status_ != DISCONNECTED &&
        connection_status_ != CONNECT_FAILED) {
      LOG(ERROR) << "Connect called on non-idle connection"
                 << " on connection: " << connection_name_
                 << ", host: " << host_
                 << ", port: " << port_
                 << ", new connect arguments: "
                 << " connection: " << name
                 << ", host: " << host
                 << ", port: " << port;
      SetError("Connecting in invalid state");
      return false;
    }

    connection_status_ = CONNECTING;
  }

  // save name/host/port for error printouts
  connection_name_ = name;
  host_ = host;
  port_ = port;
  user_ = user;
  protocol_ = protocol;

  const char *passwd = nullptr;
  const char *db = nullptr;
  const char *socket = nullptr;
  int flags = 0;

  if (compress_) {
    flags |= CLIENT_COMPRESS;
  }

  unsigned int opt_protocol = ParseProtocolString(protocol);
  mysql_options(mysql_.get(), MYSQL_OPT_PROTOCOL, (char*) &opt_protocol);

  MYSQL *res = mysql_real_connect(
      mysql_.get(), host, user, passwd, db, port, socket, flags);

  if (res == nullptr) {
    SetError(std::string("Failed to connect: ").
             append(mysql_error(mysql_.get())));
    return false;
  }

  do {
    absl::MutexLock lock(&mutex_);

    if (connection_status_ != CONNECTING) {
      // Call disconnect wo/ mutex held.
      break;
    }

    if (strcasestr(mysql_->server_version, "maria")) {
      mysql_variant_ = MYSQL_VARIANT_MARIADB;
    } else {
      mysql_variant_ = MYSQL_VARIANT_MYSQL;
    }

    connection_status_ = CONNECTED;
    DLOG(INFO) << "connected to host: " << host << ", port: " << port;
    return true;
  } while (0);

  // Call disconnect wo/ mutex held.
  Disconnect();
  return false;
}

void ClientConnection::Abort() {
  absl::MutexLock lock(&mutex_);
  switch (connection_status_) {
    case ERROR_STATUS:
    case DISCONNECTED:
    case DISCONNECTING:
    case ABORTING:
    case CONNECT_FAILED:
      // do nothing
      return;
    case CONNECTING:
    case CONNECTED:
      // TODO(pivanof): Need opensource version of force-disconnect.
      break;
  }
  connection_status_ = ABORTING;
}

bool ClientConnection::FetchServerVersion() {
  if (sscanf(mysql_->server_version,
             "%d.%d.%d",
             &server_version_.major_version,
             &server_version_.minor_version,
             &server_version_.patch_level) != 3) {
    return false;
  }

  const char *comment = strchr(mysql_->server_version, '-');
  if (comment != nullptr) {
    server_version_.comment = comment + 1;
  }
  return true;
}

bool ClientConnection::FetchServerId() {
  std::string query = "SHOW VARIABLES LIKE 'SERVER_ID'";
  if (mysql_real_query(mysql_.get(), query.c_str(), query.length()))
    return false;

  std::unique_ptr<MYSQL_RES, mysql_util::MYSQL_RES_deleter> result(
      mysql_store_result(mysql_.get()));
  if (result.get() == nullptr)
    return false;

  MYSQL_ROW row = mysql_fetch_row(result.get());
  if (!absl::SimpleAtoi(row[1], &server_id_.server_id)) {
    return false;
  }

  if (mysql_variant_ == MYSQL_VARIANT_MYSQL) {
    // TODO(jonaso): Fetch UUID
  }

  return true;
}

bool ClientConnection::FetchVariable(const std::string& variable,
                                     std::string *return_value) {
  return_value->clear();
  std::string query = "SHOW VARIABLES LIKE '" + variable + "'";
  if (mysql_real_query(mysql_.get(), query.c_str(), query.length()))
    return false;

  std::unique_ptr<MYSQL_RES, mysql_util::MYSQL_RES_deleter> result(
      mysql_store_result(mysql_.get()));
  if (result.get() == nullptr)
    return false;

  MYSQL_ROW row = mysql_fetch_row(result.get());
  if (row == nullptr)
    return false;

  return_value->assign(row[1]);
  return true;
}

// Read a packet.
// The returned packet is valid until next call.
// On error a packet with len = -1 is returned.
// This method is blocking.
Connection::Packet ClientConnection::ReadPacket() {
  ulong len = cli_safe_read(mysql_.get());
  if (len == packet_error) {
    SetError(std::string("Got error reading packet from server: ").
             append(mysql_error(mysql_.get())));
    Packet err = { -1, nullptr };
    return err;
  }

  bytes_received += len;
  Packet p = { static_cast<int>(len), mysql_->net.read_pos };
  return p;
}

void ClientConnection::Disconnect() {
  {
    absl::MutexLock lock(&mutex_);
    connection_status_ = DISCONNECTING;
  }
  my_bool save = mysql_->free_me;
  mysql_->free_me = 0;
  mysql_close(mysql_.get());
  mysql_->free_me = save;

  absl::MutexLock lock(&mutex_);
  mysql_variant_ = MYSQL_VARIANT_UNKNOWN;
  server_version_ = { 0, 0, 0, "" };
  connection_status_ = DISCONNECTED;
}

// Get server version from a connected mysqld.
ClientConnection::ServerVersion ClientConnection::GetServerVersion() {
  if (server_version_.major_version == 0) {
    FetchServerVersion();
  }
  return server_version_;
}

ServerId ClientConnection::GetServerId() {
  if (server_id_.server_id == 0) {
    FetchServerId();
  }
  return server_id_;
}

// Start replication stream.
bool ClientConnection::StartReplicationStream(const GTIDList& pos,
                                              bool semi_sync) {
  switch (mysql_variant_) {
    case MYSQL_VARIANT_MYSQL:
      return StartReplicationStreamMysql(pos, semi_sync);
    case MYSQL_VARIANT_MARIADB:
      return StartReplicationStreamMariaDB(pos, semi_sync);
    case MYSQL_VARIANT_UNKNOWN:
      return false;
  }
  return false;
}

bool ClientConnection::ExecuteStatement(const std::string& query) {
  if (mysql_real_query(mysql_.get(), query.c_str(), query.length())) {
    return false;
  }
  std::unique_ptr<MYSQL_RES, mysql_util::MYSQL_RES_deleter> result(
      mysql_store_result(mysql_.get()));
  return true;
}

bool ClientConnection::ExecuteSetHeartbeatPeriod(double seconds) {
  uint64_t period_ns = static_cast<uint64_t>(seconds * 1000000000UL);
  std::string query =
      "SET @master_heartbeat_period = " + std::to_string(period_ns);

  if (!ExecuteStatement(query)) {
    LOG(ERROR) << "Failed to set hearbeat period"
               << " on connection: " << connection_name_
               << ", host: " << host_
               << ", port: " << port_;
    return false;
  }
  return true;
}

bool ClientConnection::StartReplicationStreamMysql(
    const GTIDList& pos,
    bool semi_sync) {

  if (!ExecuteSetHeartbeatPeriod(heartbeat_period_)) {
    return false;
  }

  {
    std::string value = "CRC32";
    std::string q = "SET @master_binlog_checksum = '" + value + "'";
    if (!ExecuteStatement(q)) {
      LOG(ERROR) << "Failed to set @master_binlog_checksum"
                 << ", value: '" << value << "'"
                 << " on connection: " << connection_name_
                 << ", host: " << host_
                 << ", port: " << port_;
    }
  }

  if (semi_sync) {
    std::string q = "SET @rpl_semi_sync_slave= 1";
    if (!ExecuteStatement(q)) {
      LOG(ERROR) << "Failed to set @rpl_semi_sync_slave"
                 << ", value: 1"
                 << " on connection: " << connection_name_
                 << ", host: " << host_
                 << ", port: " << port_;
      return false;
    }
  }

  COM_Binlog_Dump_GTID args;
  args.flags = 0;
  args.server_id = FLAGS_ripple_server_id;
  args.position = kStartPos;
  compat::Convert(pos, &args.gtid_executed);

  Buffer buf;
  Protocol::Pack(args, &buf);

  if (simple_command(
          mysql_.get(),
          static_cast<enum_server_command>(constants::COM_BINLOG_DUMP_GTID),
          buf.data(), buf.size(), 1)) {
    SetError(std::string("Got fatal error sending the log dump command err: ").
             append(mysql_error(mysql_.get())));
    return false;
  }
  return true;
}

bool ClientConnection::CheckSupportsSemiSync(bool *supported,
                                             bool *enabled) {
  *supported = false;
  *enabled = false;
  std::string q =
      "SHOW VARIABLES WHERE Variable_Name LIKE 'rpl_semi_sync_master_enabled'";
  if (mysql_real_query(mysql_.get(), q.c_str(), q.length())) {
    return false;
  }

  std::unique_ptr<MYSQL_RES, mysql_util::MYSQL_RES_deleter> result(
      mysql_store_result(mysql_.get()));
  if (result.get() == nullptr) {
    return true;
  }

  MYSQL_ROW row = mysql_fetch_row(result.get());
  if (row == nullptr) {
    return true;
  }

  *supported = true;
  if (strcasecmp("ON", row[1]) == 0) {
    *enabled = true;
  }

  return true;
}

bool ClientConnection::StartReplicationStreamMariaDB(
    const GTIDList& pos,
    bool semi_sync) {

  if (!ExecuteSetHeartbeatPeriod(heartbeat_period_)) {
    return false;
  }

  GTIDStartPosition start_pos;
  mysql::compat::Convert(pos, &start_pos);
  std::string start_pos_string;
  start_pos.ToMariaDBConnectState(&start_pos_string);

  if (!start_pos_string.empty()) {
    std::string q =
        "SET @slave_connect_state=\'" + start_pos_string + "\'";
    if (!ExecuteStatement(q)) {
      LOG(ERROR) << "Failed to set @slave_connect_state"
                 << ", value: '" << start_pos_string << "'"
                 << " on connection: " << connection_name_
                 << ", host: " << host_
                 << ", port: " << port_;
      return false;
    }
  }

  {
    std::string q =
        "SET @mariadb_slave_capability = " +
        std::to_string(constants::MARIADB_SLAVE_CAPABILITY);
    if (!ExecuteStatement(q)) {
      LOG(ERROR) << "Failed to set @mariadb_slave_capability"
                 << ", value: '"
                 << std::to_string(constants::MARIADB_SLAVE_CAPABILITY)
                 << "'"
                 << " on connection: " << connection_name_
                 << ", host: " << host_
                 << ", port: " << port_;
    }
  }

  {
    std::string value = "CRC32";
    std::string q = "SET @master_binlog_checksum = '" + value + "'";
    if (!ExecuteStatement(q)) {
      LOG(ERROR) << "Failed to set @master_binlog_checksum"
                 << ", value: '" << value << "'"
                 << " on connection: " << connection_name_
                 << ", host: " << host_
                 << ", port: " << port_;
    }
  }

  if (semi_sync) {
    std::string q = "SET @rpl_semi_sync_slave= 1";
    if (!ExecuteStatement(q)) {
      LOG(ERROR) << "Failed to set @rpl_semi_sync_slave"
                 << ", value: 1"
                 << " on connection: " << connection_name_
                 << ", host: " << host_
                 << ", port: " << port_;
      return false;
    }
  }

  uint8_t buf[128];
  uint16_t binlog_flags = 0;
  int start_position = kStartPos;
  int server_id = FLAGS_ripple_server_id;

  byte_order::store4(buf + 0, start_position);
  byte_order::store2(buf + 4, binlog_flags);
  byte_order::store4(buf + 6, server_id);
  buf[10] = 0;  // empty filename

  if (simple_command(
          mysql_.get(),
          static_cast<enum_server_command>(constants::COM_BINLOG_DUMP),
          buf, 10, 1)) {
    SetError(std::string("Got fatal error sending the log dump command err: ").
             append(mysql_error(mysql_.get())));
    return false;
  }
  return true;
}

// Write a packet.
// This method is blocking.
bool ClientConnection::WritePacket(Packet packet) {
  if (my_net_write(&mysql_.get()->net, packet.ptr, packet.length)) {
    SetError("Failed to write packet");
    return false;
  }

  if (net_flush(&mysql_.get()->net)) {
    SetError("Failed net_flush after my_net_write");
    return false;
  }

  bytes_sent += packet.length;
  return true;
}

void ClientConnection::Reset() {
  mysql_.get()->net.pkt_nr = 0;
}

unsigned ClientConnection::ParseProtocolString(const char *str) {
  if (strcasecmp(str, "tcp") == 0) {
    return MYSQL_PROTOCOL_TCP;
  }
  return MYSQL_PROTOCOL_DEFAULT;
}

}  // namespace mysql

}  // namespace mysql_ripple
