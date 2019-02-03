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

#include "mysql_slave_session.h"

#include <cstdint>

#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"
#include "byte_order.h"
#include "flags.h"
#include "logging.h"
#include "monitoring.h"
#include "mysql_compat.h"
#include "mysql_init.h"
#include "mysql_protocol.h"
#include "resultset.h"

namespace mysql_ripple {

namespace mysql {

SlaveSession::SlaveSession(RippledInterface *rippled,
                           ServerConnection *connection,
                           BinlogReader::BinlogInterface *binlog,
                           FactoryInterface *factory)
    : Session(Session::MysqlSlaveSession),
      rippled_(rippled),
      binlog_reader_(rippled->GetFileFactory(), binlog),
      connection_(connection),
      factory_(factory),
      server_id_(0),
      heartbeat_period_(absl::InfiniteDuration()) {
  SetState(STARTING);
}

SlaveSession::~SlaveSession() {
  // factory_->EndSession must be called before any other work in the
  // destructor so that the management session will not iterate over
  // this slave session and introspect into potentially deleted data.
  // (e.g. call getHost or getPort on a deleted connection.)
  factory_->EndSession(this);
  delete connection_;
}

void SlaveSession::SetConnectionStatusMetrics() {
  std::string status = Connection::to_string(connection_->connection_status());
  std::string last_err = connection_->HasError() ?
    connection_->GetLastErrorMessage() : "";
  monitoring::slave_connection_status->Set(status, GetServerName());
  monitoring::last_slave_connect_error->Set(last_err, GetServerName());
  monitoring::bytes_sent_to_slave->Set(connection_->GetBytesSent(),
    GetServerName());
  monitoring::bytes_received_from_slave->Set(connection_->GetBytesReceived(),
    GetServerName());
}


void SlaveSession::Run() {
  ThreadInit();
  SetState(STARTED);

  LOG(INFO) << "Slave session starting";

  if (!Authenticate()) {
    LOG(WARNING) << "Failed to authenticate";
    SetState(STOPPING);
  }

  while (!ShouldStop()) {
    connection_->Reset();
    Connection::Packet p = connection_->ReadPacket();
    if (p.length <= 0)
      break;

    switch (p.ptr[0]) {
      case constants::COM_QUIT:
        SetState(STOPPING);
        break;
      case constants::COM_QUERY:
        if (!HandleQuery(reinterpret_cast<const char*>(p.ptr + 1)))
          SetState(STOPPING);
        break;
      case constants::COM_REGISTER_SLAVE:
        if (!HandleRegisterSlave(p))
          SetState(STOPPING);
        break;
      case constants::COM_BINLOG_DUMP:
        if (!HandleBinlogDump(p))
          SetState(STOPPING);
        break;
      case constants::COM_BINLOG_DUMP_GTID:
        if (!HandleBinlogDumpGtid(p))
          SetState(STOPPING);
        break;
      case constants::COM_INIT_DB:
        if (!protocol_->SendOK())
          SetState(STOPPING);
        break;
      default:
        LOG(ERROR) << "Unhandled command: " <<
          static_cast<unsigned>(p.ptr[0]);
        SetState(STOPPING);
        break;
    }
  }

  if (server_id_ != 0) {
    rippled_->FreeServerId(server_id_);
    server_id_ = 0;
  }

  LOG(INFO) << "Slave session stopping";

  SetState(STOPPING);
  connection_->Disconnect();
  SetState(STOPPED);
  ThreadDeinit();
}

void SlaveSession::Stop() {
  SetState(STOPPING);
  connection_->Abort();
}

void SlaveSession::Unref() {
  // no one keeps reference to us, so delete self once
  // session is disconnected
  delete this;
}

bool SlaveSession::Authenticate() {
  protocol_.reset(new Protocol(connection_));
  if (!protocol_->Authenticate()) {
    return false;
  }
  return true;
}

// Function producing a UNIX_TIMESTAMP.
static int fun_UNIX_TIMESTAMP(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  context->storage.push_back(std::to_string(time(0)));
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

// Function for server id
// returns the value in column 1
static int fun_SERVER_ID(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  context->storage.push_back(std::to_string(FLAGS_ripple_server_id));
  context->resultset->rows[0][1].data = context->storage.at(0).c_str();
  return 1;
}

// Function for server id
// returns the value in column 0
static int fun_GLOBAL_SERVER_ID(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  context->storage.push_back(std::to_string(FLAGS_ripple_server_id));
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

// Function for server uuid
// returns the value in column 1
static int fun_SERVER_UUID(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  context->storage.push_back(
      context->slave_session->GetRippledUuid().ToString());
  context->resultset->rows[0][1].data = context->storage.at(0).c_str();
  return 1;
}

// Function for server uuid
// returns the value in column 0
static int fun_GLOBAL_SERVER_UUID(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  context->storage.push_back(
      context->slave_session->GetRippledUuid().ToString());
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

// Function for server name
static int fun_SERVER_NAME(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  context->storage.push_back(
      context->slave_session->GetRippledServerName());
  context->resultset->rows[0][1].data = context->storage.at(0).c_str();
  return 1;
}

// Function for binlog pos
static int fun_GTID_BINLOG_POS(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;

  std::string str;
  BinlogPosition pos = context->slave_session->GetBinlogPosition();
  GTIDStartPosition start_pos;
  mysql::compat::Convert(pos.gtid_start_position, &start_pos);
  start_pos.ToMariaDBConnectState(&str);

  context->storage.push_back(str);
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

// Function for gtid_executed.
static int fun_GTID_EXECUTED(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;

  BinlogPosition pos = context->slave_session->GetBinlogPosition();
  GTIDSet gtid_executed;
  compat::Convert(pos.gtid_start_position, &gtid_executed);

  context->storage.push_back(gtid_executed.ToString());
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

// Function for gtid_purged
static int fun_GTID_PURGED(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;

  BinlogPosition pos = context->slave_session->GetBinlogPosition();

  context->storage.push_back(pos.gtid_purged.ToString());
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

static int fun_SHUTDOWN(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  context->slave_session->HandleShutdown();
  context->storage.push_back("OK");
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

static int fun_START_SLAVE(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  std::string msg;
  if (!context->slave_session->HandleStartSlave(&msg))
    context->storage.push_back(msg);
  else
    context->storage.push_back("OK");
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

static int fun_STOP_SLAVE(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  std::string msg;
  if (!context->slave_session->HandleStopSlave(&msg))
    context->storage.push_back(msg);
  else
    context->storage.push_back("OK");
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

static int fun_FLUSH_BINARY_LOGS(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  std::string new_file;
  if (context->slave_session->HandleFlushLogs(&new_file))
    context->storage.push_back(new_file);
  else
    context->storage.push_back("Failed to flush logs");
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

static int fun_PURGE_BINARY_LOGS(resultset::QueryContext *context) {
  if (context->row_no > 0)
    return 0;
  std::string oldest_file;
  if (context->slave_session->HandlePurgeLogs(&oldest_file))
    context->storage.push_back(oldest_file);
  else
    context->storage.push_back("Failed to purge logs");
  context->resultset->rows[0][0].data = context->storage.at(0).c_str();
  return 1;
}

// Function producing a binlog stream
int fun_SHOW_BINLOG_EVENTS(resultset::QueryContext *context) {
  if (context->row_no == 0) {
    GTIDList start_pos;
    std::string message;
    if (!context->slave_session->binlog_reader_.Open(&start_pos, &message)) {
      LOG(ERROR) << "SHOW BINLOG EVENTS failed: " << message;
      return 0;
    }
  }

  RawLogEventData event;
  if (context->slave_session->binlog_reader_.ReadEvent(
          &event, absl::ZeroDuration()) != file_util::READ_OK) {
    context->slave_session->binlog_reader_.Close();
    return 0;
  }

  if (event.header.event_length == 0) {
    context->slave_session->binlog_reader_.Close();
    return 0;
  }

  BinlogPosition pos = context->slave_session->binlog_reader_.
      GetBinlogPosition();

  context->storage.clear();
  context->storage.push_back(pos.latest_event_start_position.filename);
  context->storage.push_back(std::to_string(
      pos.latest_event_start_position.offset));
  context->storage.push_back(constants::ToString(
      static_cast<constants::EventType>(event.header.type)));
  context->storage.push_back(std::to_string(event.header.server_id));
  context->storage.push_back(std::to_string(
      pos.latest_event_end_position.offset));
  context->storage.push_back(event.ToInfoString());

  for (int i = 0; i < 6; i++) {
    context->resultset->rows[0][i].data = context->storage.at(i).c_str();
  }

  return 1;
}

struct HardcodedQuery {
  const char * query;
  const resultset::Resultset resultset;
};

static HardcodedQuery hardcoded_queries[] = {
    {"SELECT DATABASE()",
     {{{nullptr, nullptr, "DATABASE()", constants::TYPE_VARCHAR, 255}},
      resultset::NullRowFunction,
      {{{nullptr}}}}},
    {"select @@version_comment limit 1",
     {{{nullptr, nullptr, "@@version_comment", constants::TYPE_VARCHAR, 255}},
      resultset::NullRowFunction,
      {{{"ripple version comment"}}}}},
    {"SELECT UNIX_TIMESTAMP()",
     {{{nullptr, nullptr, "UNIX_TIMESTAMP()", constants::TYPE_VARCHAR, 255}},
      fun_UNIX_TIMESTAMP,
      {{{nullptr}}}}},
    {"SHOW VARIABLES LIKE 'SERVER_ID'",
     {{{nullptr, nullptr, "Variable_name", constants::TYPE_VARCHAR, 255},
       {nullptr, nullptr, "Value", constants::TYPE_VARCHAR, 255}},
      fun_SERVER_ID,
      {{{"server_id"}, {nullptr}}}}},
    {"SHOW VARIABLES LIKE 'SERVER_UUID'",
     {{{nullptr, nullptr, "Variable_name", constants::TYPE_VARCHAR, 255},
       {nullptr, nullptr, "Value", constants::TYPE_VARCHAR, 255}},
      fun_SERVER_UUID,
      {{{"server_uuid"}, {nullptr}}}}},
    {"SHOW VARIABLES LIKE 'server_name'",
     {{{nullptr, nullptr, "Variable_name", constants::TYPE_VARCHAR, 255},
       {nullptr, nullptr, "Value", constants::TYPE_VARCHAR, 255}},
      fun_SERVER_NAME,
      {{{"server_name"}, {nullptr}}}}},
    {"SELECT @master_binlog_checksum",
     {{{nullptr, nullptr, "@master_binlog_checksum", constants::TYPE_VARCHAR,
        255}},
      resultset::NullRowFunction,
      {{{"CRC32"}}}}},
    {"SELECT @@GLOBAL.SERVER_ID",
     {{{nullptr, nullptr, "@@GLOBAL.SERVER_ID", constants::TYPE_VARCHAR, 255}},
      fun_GLOBAL_SERVER_ID,
      {{{nullptr}}}}},
    {"SELECT @@GLOBAL.SERVER_UUID",
     {{{nullptr, nullptr, "@@GLOBAL.SERVER_UUID", constants::TYPE_VARCHAR, 255}},
      fun_GLOBAL_SERVER_UUID,
      {{{nullptr}}}}},
    {"SELECT @@GLOBAL.gtid_domain_id",
     {{{nullptr, nullptr, "@@GLOBAL.gtid_domain_id", constants::TYPE_VARCHAR,
        255}},
      resultset::NullRowFunction,
      {{{"0"}}}}},
    {"SELECT @@gtid_binlog_pos",
     {{{nullptr, nullptr, "@@gtid_binlog_pos", constants::TYPE_VARCHAR, 255}},
      fun_GTID_BINLOG_POS,
      {{{nullptr}}}}},
    {"SELECT @@global.gtid_executed",
     {{{nullptr, nullptr, "@@global.gtid_executed", constants::TYPE_VARCHAR,
        255}},
      fun_GTID_EXECUTED,
      {{{nullptr}}}}},
    {"SELECT @@gtid_purged",
     {{{nullptr, nullptr, "@@gtid_purged", constants::TYPE_VARCHAR, 255}},
      fun_GTID_PURGED,
      {{{nullptr}}}}},
    {"SELECT @@GLOBAL.GTID_MODE",
     {{{nullptr, nullptr, "@@GLOBAL.GTID_MODE", constants::TYPE_VARCHAR, 255}},
      resultset::NullRowFunction,
      {{{"ON"}}}}},
    {"SHOW VARIABLES WHERE Variable_Name LIKE 'rpl_semi_sync_master_enabled'"
     " OR Variable_Name LIKE 'rpl_semi_sync_enabled'",
     {{{nullptr, nullptr, "Variable_name", constants::TYPE_VARCHAR, 255},
       {nullptr, nullptr, "Value", constants::TYPE_VARCHAR, 255}},
      resultset::NullRowFunction,
      {
          // TODO(jonas) : { { "rpl_semi_sync_master_enabled" }, { "ON" } }
      }}},
    {"SET NAMES latin1", resultset::Resultset()},
    {"SHUTDOWN",
     {{
          {nullptr, nullptr, "SHUTDOWN", constants::TYPE_VARCHAR, 255},
      },
      fun_SHUTDOWN,
      {{{nullptr}}}}},
    {"SHOW BINLOG EVENTS",
     {{{nullptr, nullptr, "Log_name", constants::TYPE_VARCHAR, 60},
       {nullptr, nullptr, "Pos", constants::TYPE_LONGLONG, 11},
       {nullptr, nullptr, "Event_type", constants::TYPE_VARCHAR, 11},
       {nullptr, nullptr, "Server_id", constants::TYPE_LONG, 10},
       {nullptr, nullptr, "End_log_pos", constants::TYPE_LONGLONG, 11},
       {nullptr, nullptr, "Info", constants::TYPE_VARCHAR, 60}},
      fun_SHOW_BINLOG_EVENTS,
      {{{nullptr}, {nullptr}, {nullptr}, {nullptr}, {nullptr}, {nullptr}}}}},
    {"START SLAVE",
     {{
          {nullptr, nullptr, "START SLAVE", constants::TYPE_VARCHAR, 255},
      },
      fun_START_SLAVE,
      {{{nullptr}}}}},
    {"STOP SLAVE",
     {{
          {nullptr, nullptr, "STOP SLAVE", constants::TYPE_VARCHAR, 255},
      },
      fun_STOP_SLAVE,
      {{{nullptr}}}}},
    {"FLUSH BINARY LOGS",
     {{
          {nullptr, nullptr, "FLUSH BINARY LOGS", constants::TYPE_VARCHAR, 255},
      },
      fun_FLUSH_BINARY_LOGS,
      {{{nullptr}}}}},
    {"PURGE BINARY LOGS",
     {{
          {nullptr, nullptr, "PURGE BINARY LOGS", constants::TYPE_VARCHAR, 255},
      },
      fun_PURGE_BINARY_LOGS,
      {{{nullptr}}}}},
    {nullptr, resultset::Resultset()}};

bool SlaveSession::HandleQuery(const char *query) {
  for (int i = 0; hardcoded_queries[i].query != nullptr; i++) {
    if (strcmp(query, hardcoded_queries[i].query) == 0) {
      if (hardcoded_queries[i].resultset.IsFixed()) {
        return protocol_->SendResultset(hardcoded_queries[i].resultset);
      }

      // Dynamic result set...
      // 1) Copy resultset for EvalQuery to operate on.
      resultset::Resultset rs = hardcoded_queries[i].resultset;
      resultset::QueryContext context;
      context.query = query;
      context.resultset = &rs;
      context.slave_session = this;
      context.row_no = 0;

      // 2) Send meta data
      if (!protocol_->SendMetaData(&rs.column_definition[0],
                                 rs.column_definition.size())) {
        return false;
      }

      // 3) produce rows
      int res;
      while ((res = rs.rowFunction(&context)) > 0) {
        if (!protocol_->SendResultRow(&rs.rows[0][0], rs.rows[0].size()))
          return false;
        context.row_no++;
      }

      // 4) send eof
      if (res != 0) {
        // error.
        return false;
      }
      return protocol_->SendEOF();
    }
  }

  if (strncasecmp(query, "SET ", 4) == 0) {
    return HandleSetQuery(query + 4);  // skip "SET "
  }
  LOG(ERROR) << "Unhandled query: '"
             << query << "'";
  return false;
}

bool SlaveSession::HandleSetQuery(const char *query) {
  const char *split = strchr(query, '=');
  if (split == nullptr) {
    return false;
  }

  std::string var(query, split - query);
  std::string value(split + 1);
  var = std::string(absl::StripAsciiWhitespace(var));
  value = std::string(absl::StripAsciiWhitespace(value));

  std::size_t found = var.find_first_not_of("@");
  if (found != std::string::npos) {
    var.erase(0, found);
  }

  variables_.erase(var);
  if (value != "nullptr") {
    std::pair<std::string, std::string> variable(var, value);
    if (variables_.insert(variable).second != true) {
      return false;
    }
  }

  return protocol_->SendOK();
}

bool SlaveSession::HandleRegisterSlave(Connection::Packet p) {
  if (!protocol_->SendOK())
    return false;
  connection_->Reset();
  return true;
}

namespace {
absl::string_view stripQuotes(absl::string_view s) {
  const char quote = '\'';
  if (s.size() >= 2 && s.front() == quote && s.back() == quote) {
    s.remove_prefix(1);
    s.remove_suffix(1);
  }
  return s;
}
}  // namespace

bool SlaveSession::HandleBinlogDump(Connection::Packet p) {
  // TODO (jonaso) : move unpacking into mysql_protocol
  uint32_t server_id_tmp = byte_order::load4(p.ptr + 7);

  if (server_id_tmp == FLAGS_ripple_server_id) {
    std::string msg =
        std::string("Slave connects with server_id (")+
        std::to_string(server_id_tmp) +
        std::string(") that is used by rippled");
    LOG(WARNING) << msg;
    protocol_->SendERR(1236, "42000", msg.c_str());
    return false;
  }

  absl::Duration timeout =
      absl::Milliseconds(FLAGS_ripple_slave_alloc_server_id_timeout);
  if (!rippled_->AllocServerId(server_id_tmp, timeout)) {
    std::string msg = std::string("Slave connects with server_id (") +
                      std::to_string(server_id_tmp) +
                      std::string(") that is used by someone else");
    LOG(WARNING) << msg;
    protocol_->SendERR(1236, "42000", msg.c_str());
    return false;
  }

  // all ok.
  server_id_ = server_id_tmp;

  auto server_name_pair = variables_.find("server_name");
  if (server_name_pair != variables_.end()) {
    std::string server_name(stripQuotes(server_name_pair->second));
    server_name_.assign(server_name);
  }

  GTIDStartPosition start_pos;
  auto slave_connect_state = variables_.find("slave_connect_state");
  if (slave_connect_state != variables_.end()) {
    if (!start_pos.ParseMariaDBConnectState(slave_connect_state->second)) {
      return false;
    }
  }

  GTIDList gtid_executed;
  gtid_executed.Assign(start_pos);
  LOG(INFO) << "Slave"
            << " server_id: " << server_id_tmp
            << " server_name: " << server_name_
            << " request to start from: '"
            << start_pos.ToString()
            << "'";

  std::string message;
  if (!binlog_reader_.Open(&gtid_executed, &message)) {
    LOG(ERROR) << "Failed to open binlog for binlog dump: " << message;
    protocol_->SendERR(1236, "42000", message.c_str());
    return false;
  }

  auto master_heartbeat_period_ns = variables_.find("master_heartbeat_period");
  if (master_heartbeat_period_ns != variables_.end()) {
    int64_t ns;
    if (absl::SimpleAtoi(master_heartbeat_period_ns->second, &ns)) {
      // Zero means no heartbeats
      heartbeat_period_ =
          ns == 0 ? absl::InfiniteDuration() : absl::Nanoseconds(ns);
    }
  }

  auto master_binlog_checksum = variables_.find("master_binlog_checksum");
  if (master_binlog_checksum != variables_.end()) {
    protocol_->SetEventChecksums(true);
  }

  bool retval = SendEvents();
  binlog_reader_.Close();
  return retval;
}

bool SlaveSession::HandleBinlogDumpGtid(Connection::Packet p) {
  mysql::COM_Binlog_Dump_GTID args;
  if (!mysql::Protocol::Unpack(p.ptr + 1, p.length - 1, &args)) {
    std::string msg = "Failed to parse arguments to COM_Binlog_Dump_GTID";
    LOG(ERROR) << msg;
    protocol_->SendERR(1236, "42000", msg.c_str());
    return false;
}

  if (args.server_id == FLAGS_ripple_server_id) {
    std::string msg =
        std::string("Slave connects with server_id (")+
        std::to_string(args.server_id) +
        std::string(") that is used by rippled");
    LOG(WARNING) << msg;
    protocol_->SendERR(1236, "42000", msg.c_str());
    return false;
  }

  absl::Duration timeout =
      absl::Milliseconds(FLAGS_ripple_slave_alloc_server_id_timeout);
  if (!rippled_->AllocServerId(args.server_id, timeout)) {
    std::string msg =
        std::string("Slave connects with server_id (")+
        std::to_string(args.server_id) +
        std::string(") that is used by someone else");
    LOG(WARNING) << msg;
    protocol_->SendERR(1236, "42000", msg.c_str());
    return false;
  }

  // all ok.
  server_id_ = args.server_id;

  std::string uuid = "<unknown>";
  auto slave_uuid = variables_.find("slave_uuid");
  if (slave_uuid != variables_.end()) {
    uuid = slave_uuid->second;
  }

  auto server_name_pair = variables_.find("server_name");
  if (server_name_pair != variables_.end()) {
    std::string server_name(stripQuotes(server_name_pair->second));
    server_name_.assign(server_name);
  }

  LOG(INFO) << "Slave"
            << " server_id: " << args.server_id
            << " server_name: " << server_name_
            << " uuid: " << uuid
            << " request to start from: '"
            << args.gtid_executed.ToString()
            << "'";

  GTIDList start_pos;
  start_pos.Assign(args.gtid_executed);

  std::string message;
  if (!binlog_reader_.Open(&start_pos, &message)) {
    LOG(ERROR) << "Failed to open binlog for binlog dump: " << message;
    protocol_->SendERR(1236, "42000", message.c_str());
    return false;
  }

  auto master_heartbeat_period_ns = variables_.find("master_heartbeat_period");
  if (master_heartbeat_period_ns != variables_.end()) {
    int64_t ns;
    if (absl::SimpleAtoi(master_heartbeat_period_ns->second, &ns)) {
      // Zero means no heartbeats
      heartbeat_period_ =
          ns == 0 ? absl::InfiniteDuration() : absl::Nanoseconds(ns);
    }
  }

  auto master_binlog_checksum = variables_.find("master_binlog_checksum");
  if (master_binlog_checksum != variables_.end()) {
    protocol_->SetEventChecksums(true);
  }

  bool retval = SendEvents();
  binlog_reader_.Close();
  return retval;
}

bool SlaveSession::SendEvents() {
  BinlogPosition binlog_position = binlog_reader_.GetBinlogPosition();
  FilePosition pos = binlog_position.latest_event_end_position;

  {
    /* Send initial rotate event */
    RotateEvent rotate_event;
    rotate_event.filename = pos.filename;
    rotate_event.offset = pos.offset;
    if (!SendArtificialEvent(&rotate_event, nullptr)) {
      return false;
    }
  }

  if (!binlog_position.master_format.IsEmpty()) {
    // When seeking, we scanned passed the format descriptor
    // we need to send it...so that slave doesn't get confused.
    if (!SendArtificialEvent(&binlog_position.master_format, nullptr)) {
      return false;
    }
  }

  do {
    if (ShouldStop())
      break;

    RawLogEventData event;
    if (binlog_reader_.ReadEvent(&event, heartbeat_period_) !=
        file_util::READ_OK) {
      LOG(ERROR) << "Failed to read event";
      return false;
    }
    if (event.header.event_length == 0) {
      // timeout, let's send a heartbeat
      if (!SendHeartbeat()) {
        return false;
      }
      continue;
    }

    if (event.header.type == constants::ET_START_ENCRYPTION)
      continue;

    if (event.header.type == constants::ET_ROTATE) {
      // these are RotateEvent sent from original master
      // which is saved so that we can track master position
      // to enable semi-sync. However, a slave does not need them!
      continue;
    }

    FilePosition event_pos =
        binlog_reader_.GetBinlogPosition().latest_event_start_position;

    if (event_pos.offset == 4) {
      // Don't send the first format descriptor (the "ripple" one),
      // as the duplicate FDs confuses slave.
      continue;
    }

    if (event_pos.filename.compare(pos.filename)) {
      RotateEvent rotate_event;
      rotate_event.filename = event_pos.filename;
      rotate_event.offset = 4;

      if (!SendArtificialEvent(&rotate_event, nullptr)) {
        return false;
      }
      pos = event_pos;
    }

    if (event.header.type == constants::ET_FORMAT_DESCRIPTION) {
      // set checksum correctly. in ripple it does not
      // depend on how binlog is stored locally.
      const_cast<uint8_t*>(event.event_data)[event.event_data_length - 1] =
          protocol_->GetEventChecksums();
    }

    if (!protocol_->SendEvent(event)) {
      return false;
    }

    monitoring::slave_current_event_timestamp->Set(event.header.timestamp,
        GetServerName());
  } while (true);

  return true;
}

bool SlaveSession::SendArtificialEvent(const EventBase *event,
                                       const FilePosition *pos) {
  Buffer buf;

  RawLogEventData log_event;
  log_event.header.flags = 0;
  log_event.header.timestamp = 0;
  log_event.header.server_id = FLAGS_ripple_server_id;
  log_event.header.type = event->GetEventType();
  log_event.header.event_length =
      log_event.header.PackLength() + event->PackLength();
  if (pos != nullptr) {
    log_event.header.nextpos = pos->offset;
  } else {
    log_event.header.nextpos = 0;
  }

  log_event.SerializeToBuffer(&buf);
  event->SerializeToBuffer(buf.data() + log_event.header.PackLength(),
                           buf.size() - log_event.header.PackLength());

  if (event->GetEventType() == constants::ET_FORMAT_DESCRIPTION) {
    // set checksum correctly. in ripple it does not
    // depend on how binlog is stored locally.
    buf.data()[log_event.header.event_length - 1] =
        protocol_->GetEventChecksums();
  }

  if (!protocol_->SendEvent(log_event)) {
    return false;
  }

  return true;
}

bool SlaveSession::SendHeartbeat() {
  Buffer buf;
  FilePosition event_pos =
      binlog_reader_.GetBinlogPosition().latest_event_end_position;

  HeartbeatEvent hb_event;
  hb_event.filename = event_pos.filename;

  return SendArtificialEvent(&hb_event, &event_pos);
}

void SlaveSession::HandleShutdown() {
  rippled_->Shutdown();
  Stop();  // stop self
}

bool SlaveSession::HandleStartSlave(std::string *msg) {
  bool idempotent = true;  // mimic mysqld behaviour
  return rippled_->StartMasterSession(msg, idempotent);
}

bool SlaveSession::HandleStopSlave(std::string *msg) {
  bool idempotent = true;  // mimic mysqld behaviour
  return rippled_->StopMasterSession(msg, idempotent);
}

bool SlaveSession::HandleFlushLogs(std::string *new_file) {
  return rippled_->FlushLogs(new_file);
}

bool SlaveSession::HandlePurgeLogs(std::string *oldest_file) {
  return rippled_->PurgeLogs(oldest_file);
}

}  // namespace mysql

}  // namespace mysql_ripple
