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

#include "mysql_master_session.h"

#include <algorithm>

#include "absl/time/clock.h"
#include "byte_order.h"
#include "flags.h"
#include "logging.h"
#include "log_event.h"
#include "monitoring.h"
#include "mysql_init.h"
#include "mysql_constants.h"
#include "mysql_protocol.h"

namespace mysql_ripple {

namespace mysql {

MasterSession::MasterSession(Binlog *binlog,
                             MasterSession::RippledInterface *rippled)
    : ThreadedSession(Session::MysqlMasterSession),
      binlog_(binlog),
      rippled_(rippled),
      host_(FLAGS_ripple_master_address),
      port_(FLAGS_ripple_master_port),
      protocol_(FLAGS_ripple_master_protocol),
      user_(FLAGS_ripple_master_user),
      compressed_protocol_(FLAGS_ripple_master_compressed_protocol),
      heartbeat_period_(FLAGS_ripple_master_heartbeat_period),
      connection_attempt_counter_(0),
      last_connection_attempt_time_(absl::InfinitePast()),
      reconnect_period_(absl::Seconds(FLAGS_ripple_master_reconnect_period)),
      reconnect_period_attempts_(FLAGS_ripple_master_reconnect_attempts),
      semi_sync_slave_reply_enabled_(FLAGS_ripple_semi_sync_slave_enabled),
      semi_sync_slave_reply_active_(false),
      connection_status_trigger_([this]() { SetConnectionStatusMetrics(); }),
      last_connected_time_(absl::InfinitePast()) {}

MasterSession::~MasterSession() {
}

void MasterSession::SetConnectionStatusMetrics() {
  if (connection_.connection_status() == Connection::CONNECTED)
    monitoring::time_since_master_last_connected->Set(0);
  else
    // Set this value in seconds
    monitoring::time_since_master_last_connected->Set(
      (absl::Now() - last_connected_time_) / absl::Seconds(1));

  monitoring::master_connection_status->Set(
    Connection::to_string(connection_.connection_status()));

  if (connection_.HasError())
    monitoring::last_master_connect_error->Set(
      connection_.GetLastErrorMessage());
  else
    monitoring::last_master_connect_error->Set("");

  monitoring::bytes_sent_to_master->Set(connection_.GetBytesSent());
  monitoring::bytes_received_from_master->Set(connection_.GetBytesReceived());
}

void MasterSession::ThrottleConnectionAttempts() {
  auto now = absl::Now();
  auto next_attempt_time = last_connection_attempt_time_ + reconnect_period_;

  if (now >= next_attempt_time) {
    // It has passed more than reconnect_period_seconds_
    connection_attempt_counter_ = 0;
    last_connection_attempt_time_ = now;
    return;
  }

  if (connection_attempt_counter_ < reconnect_period_attempts_) {
    // We have made less than reconnect_period_attempts_
    connection_attempt_counter_++;
    return;
  }

  WaitState(STOPPING, std::min(next_attempt_time - now, reconnect_period_));
}

void MasterSession::ResetThrottleConnectionAttempts() {
  connection_attempt_counter_ = 0;
  last_connection_attempt_time_ = absl::InfinitePast();
}

bool MasterSession::Connect() {
  ThrottleConnectionAttempts();

  // Recheck state since ThrottleConnectionAttempts might have slept
  if (ShouldStop()) {
    return false;
  }

  last_connected_time_ = absl::Now();

  connection_.SetCompressed(compressed_protocol_);
  connection_.SetHeartbeatPeriod(heartbeat_period_);
  if (connection_.Connect("master",
                          host_.c_str(),
                          port_,
                          protocol_.c_str(),
                          user_.c_str()) &&
      connection_.FetchServerVersion() &&
      connection_.FetchServerId()) {
    connection_.FetchVariable("server_name", &server_name_);
    {
      // Set own server name at master
      std::string q =
          "SET @server_name='" + FLAGS_ripple_server_name + "'";
      if (!connection_.ExecuteStatement(q)) {
        LOG(ERROR) << "Failed to set @server_name"
                   << ", value: '" << FLAGS_ripple_server_name << "'";

        connection_.Disconnect();
        return false;
      }
    }

    LOG(INFO) << "Connected to host: "
              << host_
              << ", port: " << port_
              << ", server_id: " << connection_.GetServerId().server_id
              << ", server_name: " << server_name_;

    uint32_t server_id_tmp = connection_.GetServerId().server_id;
    if (server_id_tmp == FLAGS_ripple_server_id) {
      LOG(WARNING)
          << "Disconnecting as master has server id("
          <<  connection_.GetServerId().server_id
          << ") that is same as rippled!";
      connection_.Disconnect();
      return false;
    }

    absl::Duration timeout =
        absl::Milliseconds(FLAGS_ripple_master_alloc_server_id_timeout);
    if (!rippled_->AllocServerId(server_id_tmp, timeout)) {
      LOG(WARNING) << "Disconnecting as master has server id("
                   << connection_.GetServerId().server_id
                   << " that is already connected to rippled!";
      connection_.Disconnect();
      return false;
    }
    return true;
  }
  LOG(WARNING)
      << "Failed to connected to"
      << " host: " << host_
      << ", port: " << port_;
  connection_.Disconnect();
  return false;
}

bool MasterSession::Stop() {
  connection_.Abort();
  return ThreadedSession::Stop();
}

void* MasterSession::Run() {
  ThreadInit();

  DLOG(INFO) << "Master session starting";

  while (!ShouldStop()) {
    if (!Connect()) {
      continue;
    }

    bool semi_sync_master_enabled_tmp;
    if (!connection_.CheckSupportsSemiSync(&semi_sync_master_supported_,
                                           &semi_sync_master_enabled_tmp)) {
      Disconnect();
      continue;
    }
    semi_sync_master_enabled_.store(semi_sync_master_enabled_tmp,
                                    std::memory_order_relaxed);
    if (!semi_sync_master_supported_) {
      LOG(WARNING) << "master does not support semi sync";
    } else if (!semi_sync_master_enabled_tmp) {
      LOG(WARNING) << "master does not have semi sync enabled";
    } else {
      LOG(INFO) << "master has semi sync enabled";
    }

    BinlogPosition pos = binlog_->GetBinlogPosition();
    GTIDList start_position = pos.gtid_start_position;
    LOG(INFO) << "start replicating from "
              << "'"
              << start_position.ToString().c_str()
              << "'";

    bool semi_sync_active = semi_sync_master_supported_ &&
                            GetSemiSyncSlaveReplyEnabled();
    semi_sync_slave_reply_active_.store(semi_sync_active);
    if (!connection_.StartReplicationStream(start_position, semi_sync_active)) {
      LOG(ERROR) << "Failed to start replication stream: "
                 << connection_.GetLastErrorMessage().c_str();
      Disconnect();
      continue;
    }


    if (!binlog_->ConnectionEstablished(&connection_)) {
      LOG(ERROR) << "Failure when establishing connection";
      last_connected_time_ = absl::Now();
      Disconnect();
      continue;
    }

    DLOG(INFO) << "Master session entering main loop";

    // first rotate event has checksum dependent on what we set when we
    // connect. we always set this to true...
    format_.checksum = true;

    if (!HandleHandshakeEvents()) {
      binlog_->ConnectionClosed(&connection_);
      last_connected_time_ = absl::Now();
      Disconnect();
      continue;
    }

    // We are now downloading binlogs; ripple client is in "START SLAVE;" mode
    monitoring::rippled_active->Set(true);

    // Then enter main loop
    while (!ShouldStop()) {
      RawLogEventData event;
      uint8_t semi_sync_reply = 0;
      if (!ReadEvent(&event, &semi_sync_reply))
        break;

      bool reply = semi_sync_reply && GetSemiSyncSlaveReplyActive();
      if (!binlog_->AddEvent(event, reply)) {
        LOG(ERROR) << "Failed to add event to binlog";
        break;
      }
      if (reply) {
        FilePosition file_pos =
            binlog_->GetBinlogPosition().latest_master_position;
        if (!SendSemiSyncReply(file_pos)) {
          LOG(WARNING) << "Failed to send semi sync reply";
          break;
        }
      }
      // Reset throttle counters now that we have processed
      // an event successfully.
      ResetThrottleConnectionAttempts();
    }
    binlog_->ConnectionClosed(&connection_);

    LOG(INFO) << "Disconnecting from master";
    Disconnect();

    // We are no longer downloading binlogs.
    monitoring::rippled_active->Set(false);
  }

  DLOG(INFO) << "Master session stopping";

  ThreadDeinit();

  return nullptr;
}

void MasterSession::Disconnect() {
  rippled_->FreeServerId(connection_.GetServerId().server_id);
  last_connected_time_ = absl::Now();
  semi_sync_slave_reply_active_.store(false);
  connection_.Disconnect();
}

bool MasterSession::ReadEvent(RawLogEventData *event,
                              uint8_t *semi_sync_reply) {
  Connection::Packet packet = connection_.ReadPacket();
  if (packet.length == -1) {
    LOG(ERROR) << "Failed to read packet: "
               << connection_.GetLastErrorMessage().c_str();
    return false;
  }

  if (packet.length == 0) {
    LOG(WARNING) << "Packet too short, length: " << packet.length;
    return false;
  }

  if (packet.length < 8 && packet.ptr[0] == 254) {
    LOG(INFO) << "Master disconnect";
    return false;
  }

  // Remove packet header.
  packet.ptr += 1;
  packet.length -= 1;

  if (GetSemiSyncSlaveReplyActive()) {
    if (packet.ptr[0] != constants::SEMI_SYNC_HEADER) {
      LOG(ERROR) << "Missing semi sync header"
                 << ", ptr[0]: " << static_cast<unsigned>(packet.ptr[0]);
      return false;
    }
    *semi_sync_reply = packet.ptr[1];
    packet.ptr += 2;
    packet.length -= 2;

    if (*semi_sync_reply) {
      if (semi_sync_master_enabled_.load(std::memory_order_relaxed) == false) {
        semi_sync_master_enabled_.store(true, std::memory_order_relaxed);
        LOG(INFO) << "master has enabled semi sync";
      }
    }
  }

  if (!event->ParseFromBuffer(packet.ptr, packet.length)) {
    LOG(ERROR) << "Failed to parse event"
               << ", packet.length: " << packet.length;
    return false;
  }

  if (event->header.type == constants::ET_FORMAT_DESCRIPTION) {
    // if master is checksum aware, it will add storage for checksum
    // at end, but only compute a real checksum if the FD itself
    // says that checksums are enabled...puh...
    RawLogEventData copy = *event;
    mysql::Protocol::StripEventChecksum(&copy);
    FormatDescriptorEvent format_event;
    if (!format_event.ParseFromRawLogEventData(copy)) {
      LOG(ERROR) << "Failed to parse format descriptor event";
      return false;
    }

    format_ = format_event;
    if (!format_.checksum) {
      mysql::Protocol::StripEventChecksum(event);
    }
  }

  if (format_.checksum) {
    if (!mysql::Protocol::VerifyAndStripEventChecksum(event)) {
      return false;
    }
  }

  return true;
}

bool MasterSession::SendSemiSyncReply(const FilePosition& master_pos) {
  Buffer buf;
  uint8_t *ptr = buf.Append(1 + 8 + master_pos.filename.size());
  byte_order::store1(ptr + 0, constants::SEMI_SYNC_HEADER);
  byte_order::store8(ptr + 1, master_pos.offset);
  master_pos.filename.copy(reinterpret_cast<char*>(ptr + 9),
                           master_pos.filename.size());

  return connection_.WritePacket(buf);
}

bool MasterSession::HandleHandshakeEvents() {
  // Master sends Rotate followed by FormatDescriptor when
  // we connect. Reverse this order when adding events to binlog
  // as we want the FD first in our local binlog.
  uint8_t semi_sync_reply = 0;
  RawLogEventData raw_rotate_event;
  if (!ReadEvent(&raw_rotate_event, &semi_sync_reply))
    return false;

  if (semi_sync_reply != 0) {
    LOG(ERROR) << "Received request for semi-sync reply on handshake packet!";
    return false;
  }

  if (raw_rotate_event.header.type != constants::ET_ROTATE) {
    LOG(ERROR) << "Received " << raw_rotate_event.ToString()
               << " as first event when expecting a RotateEvent";
    return false;
  }

  RotateEvent rotate_event;
  if (!rotate_event.ParseFromRawLogEventData(raw_rotate_event)) {
    LOG(ERROR) << "Failed to parse rotate event";
    return false;
  }

  // Save Rotate event so that we can add it to binlog later.
  Buffer buf;
  raw_rotate_event = raw_rotate_event.DeepCopy(&buf);

  // Now read second event.
  RawLogEventData event;
  if (!ReadEvent(&event, &semi_sync_reply))
    return false;

  // It should be a FD.
  if (event.header.type != constants::ET_FORMAT_DESCRIPTION) {
    LOG(ERROR) << "Received " << event.ToString()
               << " as second event when expecting a FormatDescriptor";
    return false;
  }

  // We don't need to reparse as ReadEvent stores FD in the
  // format_ member variable.

  // Now add them to binlog with FD first and Rotate then
  if (!binlog_->AddEvent(event, true)) return false;

  return binlog_->AddEvent(raw_rotate_event, true);
}

void MasterSession::SetSemiSyncSlaveReplyEnabled(bool onoff) {
  semi_sync_slave_reply_enabled_.store(onoff, std::memory_order_relaxed);
}

bool MasterSession::GetSemiSyncSlaveReplyEnabled() const {
  return semi_sync_slave_reply_enabled_.load(std::memory_order_relaxed);
}

bool MasterSession::GetSemiSyncSlaveReplyActive() const {
  return semi_sync_slave_reply_active_.load(std::memory_order_relaxed);
}

bool MasterSession::GetSemiSyncMasterEnabled() const {
  return semi_sync_master_enabled_.load(std::memory_order_relaxed);
}


}  // namespace mysql

}  // namespace mysql_ripple
