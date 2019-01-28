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

#ifndef MYSQL_RIPPLE_MYSQL_SLAVE_SESSION_H
#define MYSQL_RIPPLE_MYSQL_SLAVE_SESSION_H

#include <string>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include "binlog.h"
#include "binlog_reader.h"
#include "executor.h"  // RunnableInterface
#include "mysql_protocol.h"
#include "mysql_server_connection.h"
#include "resultset.h"
#include "session.h"

namespace mysql_ripple {

namespace mysql {

// A class representing a slave connecting to ripple
class SlaveSession : public Session, public RunnableInterface {
 public:
  // Interfaces used.
  class RippledInterface {
   public:
    virtual ~RippledInterface() {}
    virtual BinlogPosition GetBinlogPosition() const = 0;
    virtual const Uuid &GetUuid() const = 0;
    virtual std::string GetServerName() const = 0;
    virtual const file::Factory &GetFileFactory() const = 0;
    virtual bool AllocServerId(uint32_t server_id, absl::Duration timeout) = 0;
    virtual void FreeServerId(uint32_t server_id) = 0;
    virtual void Shutdown() = 0;
    virtual bool StartMasterSession(std::string *msg, bool idempotent) = 0;
    virtual bool StopMasterSession(std::string *msg, bool idempotent) = 0;
    virtual bool FlushLogs(std::string *new_file) = 0;
    virtual bool PurgeLogs(std::string *oldest_file) = 0;
  };

  class FactoryInterface {
   public:
    virtual ~FactoryInterface() {}
    virtual void EndSession(SlaveSession *) = 0;
  };

  SlaveSession(RippledInterface *rippled, mysql::ServerConnection *connection,
               BinlogReader::BinlogInterface *binlog,
               FactoryInterface *factory);
  virtual ~SlaveSession();

  // RunnableInterface
  void Run() override;
  void Stop() override;
  void Unref() override;

  // Attach a connected (but not authenticated) connection to this session.
  bool Authenticate();
  bool HandleQuery(const char *query);
  bool HandleSetQuery(const char *query);
  bool HandleRegisterSlave(Connection::Packet p);
  bool HandleBinlogDump(Connection::Packet p);
  bool HandleBinlogDumpGtid(Connection::Packet p);

  void HandleShutdown();
  bool HandleStartSlave(std::string *msg);
  bool HandleStopSlave(std::string *msg);

  // on success return name of new file in new_file.
  bool HandleFlushLogs(std::string *new_file);

  // on success return name of oldest kept file in oldest_file.
  bool HandlePurgeLogs(std::string *oldest_file);

  BinlogPosition GetBinlogPosition() const {
    return rippled_->GetBinlogPosition();
  }

  BinlogPosition GetSlaveBinlogPosition() const {
    return binlog_reader_.GetBinlogPosition();
  }

  const Uuid& GetRippledUuid() const {
    return rippled_->GetUuid();
  }

  std::string GetRippledServerName() const {
    return rippled_->GetServerName();
  }

  std::string GetHost() const { return connection_->GetHost(); }
  uint16_t GetPort() const { return connection_->GetPort(); }
  uint32_t GetServerId() const { return server_id_; }
  std::string GetServerName() const { return server_name_; }

  void SetConnectionStatusMetrics();

 private:
  bool SendHeartbeat();

  // Encapsulate the event with a LogEventHeader and send it
  // using protocol->SendEvent().
  // LogEventHeader is constructed so that has server_id = rippled_id
  // and has timestamp = 0.
  bool SendArtificialEvent(const EventBase* ev, const FilePosition *pos);

  // Read from binlog_reader_ and send events to slave.
  bool SendEvents();

  RippledInterface *rippled_;
  BinlogReader binlog_reader_;
  mysql::ServerConnection* connection_;
  std::unique_ptr<mysql::Protocol> protocol_;
  FactoryInterface *factory_;

  // session variables.
  absl::flat_hash_map<std::string, std::string> variables_;

  uint32_t server_id_;
  absl::Duration heartbeat_period_;
  std::string server_name_;

  SlaveSession(SlaveSession&&) = delete;
  SlaveSession(const SlaveSession&) = delete;
  SlaveSession& operator=(SlaveSession&&) = delete;
  SlaveSession& operator=(const SlaveSession&) = delete;

  friend int fun_SHOW_BINLOG_EVENTS(resultset::QueryContext *context);
};

}  // namespace mysql

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_SLAVE_SESSION_H
