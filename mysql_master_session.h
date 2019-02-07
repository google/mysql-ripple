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

#ifndef MYSQL_RIPPLE_MYSQL_MASTER_SESSION_H
#define MYSQL_RIPPLE_MYSQL_MASTER_SESSION_H

#include <atomic>

#include "binlog.h"
#include "monitoring.h"
#include "mysql_client_connection.h"
#include "session.h"

namespace mysql_ripple {

namespace mysql {

// A class representing a mysql active connection to a mysql master
class MasterSession : public ThreadedSession {
 public:
  class RippledInterface {
   public:
    virtual ~RippledInterface() {}
    virtual bool AllocServerId(uint32_t server_id, absl::Duration timeout) = 0;
    virtual void FreeServerId(uint32_t server_id) = 0;
  };

  MasterSession(Binlog* binlog, RippledInterface* rippled);
  virtual ~MasterSession();

  // Return a const connection that can be inspected
  // but not modified.
  virtual const mysql::ClientConnection& GetConnection() const {
    return connection_;
  }

  bool Stop() override;

  // Does master have semi-sync enabled.
  // This value is set during connect.
  bool GetSemiSyncMasterEnabled() const;

  // Are we allowed to request semi-sync from master.
  // This can be turned on/off while a connection is ongoing.
  // Only makes sense when master has semi-sync enabled.
  void SetSemiSyncSlaveReplyEnabled(bool onoff);
  bool GetSemiSyncSlaveReplyEnabled() const;

  // Did we request semi-syncing from master and are we currently sending
  // semi-sync acks.
  bool GetSemiSyncSlaveReplyActive() const;

  // Get the server name that was read from master at connect.
  std::string GetServerName() const {
    return server_name_;
  }

  // Lock/unlock master session.
  // This prevents master from starting if stopped,
  // and make checking of state, and setting below race-free.
  void Lock() const EXCLUSIVE_LOCK_FUNCTION(mutex_) { mutex_.Lock(); }
  void Unlock() const UNLOCK_FUNCTION(mutex_) { mutex_.Unlock(); }

  // Set properties for master connection.
  // Changes take effect next time we connect.
  void SetHost(const std::string& host) { host_ = host; }
  void SetPort(int port) { port_ = port; }
  void SetProtocol(const std::string& protocol) { protocol_ = protocol; }
  void SetUser(const std::string& user) { user_ = user; }
  void SetPassword(const std::string& password) { password_ = password; }
  void SetCompressedProtocol(bool onoff) { compressed_protocol_ = onoff; }
  void SetHeartbeatPeriod(double period) { heartbeat_period_ = period; }

  std::string GetHost() const { return host_; }
  int GetPort() const { return port_; }
  std::string GetProtocol() const { return protocol_; }
  std::string GetUser() const { return user_; }
  std::string GetPassword() const { return password_; }
  bool GetCompressedProtocol() const { return compressed_protocol_; }
  double GetHeartbeatPeriod() const { return heartbeat_period_; }

 private:
  Binlog *binlog_;
  RippledInterface* rippled_;
  mysql::ClientConnection connection_;

  // Connection properties.
  // Set in constructor from FLAGS_.
  // Modifiable by user.
  // Used when connecting.
  std::string host_;
  int port_;
  std::string protocol_;
  std::string user_;
  std::string password_;
  bool compressed_protocol_;
  double heartbeat_period_;

  // Needed to parse events :(
  FormatDescriptorEvent format_;

  std::string server_name_;

  // Wait for connection settings and then connect.
  bool Connect();

  void Disconnect();

  // Handle handshake events, i.e Rotate+FormatDescriptor that
  // is sent by master when we connect.
  bool HandleHandshakeEvents();

  // Read one event.
  bool ReadEvent(RawLogEventData *event, uint8_t *semi_sync_reply);

  bool SendSemiSyncReply(const FilePosition& master_pos);

  // Throttle connection attempts so that we don't spin and try to connect.
  void ThrottleConnectionAttempts();
  // Reset counters after successfully having added things to binlog.
  void ResetThrottleConnectionAttempts();

 protected:
  void* Run();

  // How many attempts have we made since last timestamp
  int connection_attempt_counter_;
  // When did we last start to connect
  absl::Time last_connection_attempt_time_;

  // How many attempts shall we make every reconnect_period_
  absl::Duration reconnect_period_;
  int reconnect_period_attempts_;

  // Does master have semi-sync plugin installed?
  // This value is set during connect and not updated after.
  bool semi_sync_master_supported_;

  // Does master have semi-sync enabled?
  // This value is set when first semi-sync ACK request is received.
  // Updates/read to variable is done using memory_order_relaxed.
  std::atomic_bool semi_sync_master_enabled_;

  // Are we allowed to request semi-syncing from master?
  // This value can be changed without reconnecting, but its change will become
  // effective only after the next reconnect.
  std::atomic_bool semi_sync_slave_reply_enabled_;

  // Did we request semi-syncing from master and do we send semi-sync acks?
  // This can be true only if we are connected to master and
  // semi_sync_slave_reply_enabled_ was true when we were connecting.
  std::atomic_bool semi_sync_slave_reply_active_;

  monitoring::CallbackTrigger connection_status_trigger_;
  absl::Time last_connected_time_;
  void SetConnectionStatusMetrics();

  MasterSession(MasterSession&&) = delete;
  MasterSession(const MasterSession&) = delete;
  MasterSession& operator=(MasterSession&&) = delete;
  MasterSession& operator=(const MasterSession&) = delete;
};

}  // namespace mysql

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_MASTER_SESSION_H
