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

#include "manager.h"

#include "mysql_client_connection.h"
#include "mysql_master_session.h"
#include "mysql_slave_session.h"

namespace mysql_ripple {

using mysql::ClientConnection;

static void CopyFilePosition(ripple_proto::FilePosition *dest,
                             FilePosition& src) {
  dest->set_filename(src.filename);
  dest->set_offset(src.offset);
}

static void CopyGTID(ripple_proto::GTID *dest, GTID& src) {
  if (!src.server_id.uuid.empty())
    dest->set_uuid(src.server_id.uuid.ToString());
  else
    dest->clear_uuid();
  dest->set_server_id(src.server_id.server_id);
  dest->set_seq_no(src.seq_no);
  dest->set_domain_id(src.domain_id);
}

static void CopyGTIDStartPosition(ripple_proto::GTIDStartPosition *dest,
                                  GTIDStartPosition& src) {
  for (GTID gtid : src.gtids) {
    CopyGTID(dest->add_gtid(), gtid);
  }
}

static void CopyBinlogPosition(ripple_proto::BinlogPosition *dest,
                               BinlogPosition& src) {
  // Copy ripple's binlog file positions for the latest recorded event,
  // next event and the latest completed GTID (on ripple).
  CopyFilePosition(dest->mutable_latest_event_start_position(),
                   src.latest_event_start_position);
  CopyFilePosition(dest->mutable_latest_event_end_position(),
                   src.latest_event_end_position);
  CopyFilePosition(dest->mutable_latest_completed_gtid_position(),
                   src.latest_completed_gtid_position);

  // Copy master's binlog file positions for the latest recorded event,
  // next event and the latest completed GTID (on ripple).
  CopyFilePosition(dest->mutable_latest_master_position(),
                   src.latest_master_position);
  CopyFilePosition(dest->mutable_next_master_position(),
                   src.next_master_position);
  CopyFilePosition(dest->mutable_latest_completed_gtid_master_position(),
                   src.latest_completed_gtid_master_position);

  CopyGTID(dest->mutable_latest_start_gtid(), src.latest_start_gtid);
  CopyGTID(dest->mutable_latest_completed_gtid(), src.latest_completed_gtid);

  GTIDStartPosition pos;
  mysql::compat::Convert(src.gtid_start_position, &pos);
  CopyGTIDStartPosition(dest->mutable_gtid_start_position(), pos);

  GTIDStartPosition gtid_purged;
  mysql::compat::Convert(src.gtid_purged, &gtid_purged);
  CopyGTIDStartPosition(dest->mutable_gtid_purged(), gtid_purged);
}

static void GetSlaveAddress(mysql::SlaveSession *session, void *cookie) {
  ripple_proto::Slaves *slaves = (ripple_proto::Slaves*) cookie;
  ripple_proto::SlaveAddress *slaveAddress = slaves->add_slave();

  slaveAddress->set_host(session->GetHost());
  slaveAddress->set_port(session->GetPort());
  slaveAddress->set_server_id(session->GetServerId());
  slaveAddress->set_server_name(session->GetServerName());
}

struct BinlogInfo {
  const ripple_proto::SlaveAddress *address;
  ripple_proto::BinlogPosition *binlogPosition;
};

static void GetBinlogPosition(mysql::SlaveSession *session, void *cookie) {
  BinlogInfo *info = (BinlogInfo*) cookie;

  std::string host = info->address->host();
  if (host.compare(session->GetHost()) != 0)
    return;
  if (info->address->port() != session->GetPort())
    return;

  BinlogPosition slave_pos = session->GetSlaveBinlogPosition();
  CopyBinlogPosition(info->binlogPosition, slave_pos);
}

void Manager::FillRippleInfo(ripple_proto::RippleInfo *info) {
  info->set_server_name(rippled_->GetServerName());
}

void Manager::FillMasterStatus(ripple_proto::MasterStatus *master_status) {
  const mysql::MasterSession &session = rippled_->GetMasterSession();
  const ClientConnection& connection = session.GetConnection();

  session.Lock();
  if (session.SessionStateLocked() == Session::STARTED ||
      session.SessionStateLocked() == Session::STARTING) {
    // Return values from the currently active connection.
    master_status->mutable_master_info()->mutable_host()
        ->set_value(connection.GetHost());
    master_status->mutable_master_info()->mutable_user()
        ->set_value(connection.GetUser());
    master_status->mutable_master_info()->mutable_port()
        ->set_value(connection.GetPort());
    master_status->mutable_master_info()->mutable_protocol()
        ->set_value(connection.GetProtocol());
  } else {
    // Return values from the session instead of last attempted connection.
    master_status->mutable_master_info()->mutable_host()
        ->set_value(session.GetHost());
    master_status->mutable_master_info()->mutable_user()
        ->set_value(session.GetUser());
    master_status->mutable_master_info()->mutable_port()
        ->set_value(session.GetPort());
    master_status->mutable_master_info()->mutable_protocol()
        ->set_value(session.GetProtocol());
  }
  master_status->mutable_master_info()->mutable_semi_sync_slave_reply_enabled()
      ->set_value(session.GetSemiSyncSlaveReplyEnabled());
  master_status->set_semi_sync_slave_reply_active(
        session.GetSemiSyncSlaveReplyActive());
  master_status->mutable_master_info()->mutable_compressed_protocol()
      ->set_value(session.GetCompressedProtocol());
  master_status->mutable_master_info()->mutable_heartbeat_period()
      ->set_value(session.GetHeartbeatPeriod());

  // Copy master connection information.
  if (connection.connection_status() == Connection::CONNECTED) {
    master_status->set_state(ripple_proto::MasterStatus::CONNECTED);

    ServerId server_id = connection.GetServerId();
    master_status->set_server_id(server_id.server_id);
    if (!server_id.uuid.empty())
      master_status->set_uuid(server_id.uuid.ToString());

    master_status->set_semi_sync_master_enabled(
        session.GetSemiSyncMasterEnabled());
    ripple_proto::ServerVersion *version =
        master_status->mutable_server_version();
    ClientConnection::ServerVersion connection_version =
        connection.GetServerVersion();
    version->set_major_version(connection_version.major_version);
    version->set_minor_version(connection_version.minor_version);
    version->set_patch_level(connection_version.patch_level);
    version->set_comment(connection_version.comment);
    master_status->set_server_name(session.GetServerName());
  } else if (session.SessionStateLocked() == Session::STARTED ||
             session.SessionStateLocked() == Session::STARTING) {
    // Thread is starting/started, but we're not yet connected.
    // Most likely we're either connecting or connect throttling.
    master_status->set_state(ripple_proto::MasterStatus::CONNECTING);
  } else {
    // If thread is stopping
    // then CONNECTION status is disconnecting or disconnected
    // and this is enumerated as DISCONNECTED
    master_status->set_state(ripple_proto::MasterStatus::DISCONNECTED);
  }
  session.Unlock();
}

void Manager::FillConnectedSlaves(ripple_proto::Slaves *slaves) {
  rippled_->GetSlaveSessionFactory().IterateSessions(GetSlaveAddress,
                                                     (void *)slaves);
}

void Manager::FillSlaveBinlogPosition(const ripple_proto::SlaveAddress& address,
                                      ripple_proto::BinlogPosition *position) {
  BinlogInfo info;
  info.address = &address;
  info.binlogPosition = position;
  rippled_->GetSlaveSessionFactory().IterateSessions(GetBinlogPosition,
                                                     (void *)&info);
}

void Manager::FillBinlogPosition(ripple_proto::BinlogPosition *position) {
  BinlogPosition binlog_position = rippled_->GetBinlogPosition();
  CopyBinlogPosition(position, binlog_position);
}

void Manager::ToggleSemiSyncReply(const ripple_proto::OnOff *onoff,
                                  ripple_proto::Status *status) {
  rippled_->GetMasterSession().SetSemiSyncSlaveReplyEnabled(onoff->on());
  status->set_code(0);
  status->clear_info();
}

void Manager::StartSlave(ripple_proto::Status *status,
                         const ripple_proto::StartSlaveRequest *request) {
  std::string msg;
  if (rippled_->StartMasterSession(&msg, request->idempotent())) {
    status->set_code(0);
    status->clear_info();
  } else {
    status->set_code(-1);
    status->set_info(msg);
  }
}

void Manager::StopSlave(ripple_proto::Status *status,
                        const ripple_proto::StopSlaveRequest *request) {
  std::string msg;
  if (rippled_->StopMasterSession(&msg, request->idempotent())) {
    status->set_code(0);
    status->clear_info();
  } else {
    status->set_code(-1);
    status->set_info(msg);
  }
}

void Manager::ChangeMaster(const ripple_proto::MasterInfo *master_info,
                           ripple_proto::Status *status) {
  mysql::MasterSession &session = rippled_->GetMasterSession();
  session.Lock();
  if (session.SessionStateLocked() != Session::INITIAL) {
    status->set_code(-1);
    status->set_info("Incorrect session state, need to stop slave before"
                     " doing change master");
    session.Unlock();
    return;
  }

  if (master_info->has_host()) {
    session.SetHost(master_info->host().value());
  }
  if (master_info->has_user()) {
    session.SetUser(master_info->user().value());
  }
  if (master_info->has_port()) {
    session.SetPort(master_info->port().value());
  }
  if (master_info->has_protocol()) {
    session.SetProtocol(master_info->protocol().value());
  }
  if (master_info->has_semi_sync_slave_reply_enabled()) {
    session.SetSemiSyncSlaveReplyEnabled(
        master_info->semi_sync_slave_reply_enabled().value());
  }
  if (master_info->has_compressed_protocol()) {
    session.SetCompressedProtocol(master_info->compressed_protocol().value());
  }
  if (master_info->has_heartbeat_period()) {
    session.SetHeartbeatPeriod(master_info->heartbeat_period().value());
  }

  session.Unlock();
  status->set_code(0);
  status->clear_info();
}

}  // namespace mysql_ripple
