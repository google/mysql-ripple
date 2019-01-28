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

#ifndef MYSQL_RIPPLE_RIPPLED_H
#define MYSQL_RIPPLE_RIPPLED_H

#include <unordered_set>

#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "binlog.h"
#include "file.h"
#include "listener.h"
#include "management_session.h"
#include "manager.h"
#include "mysql_master_session.h"
#include "mysql_server_port.h"
#include "mysql_slave_session.h"
#include "purge_thread.h"
#include "session_factory.h"

namespace mysql_ripple {

class Manager;

// This class is contains the ripple components.
class Rippled : mysql::SlaveSession::RippledInterface,
                mysql::MasterSession::RippledInterface,
                Manager::RippledInterface {
 public:
  Rippled();
  virtual ~Rippled();

  bool Setup();
  bool Start();
  void Shutdown() override;
  void WaitShutdown();
  void Teardown();

  BinlogPosition GetBinlogPosition() const override;

  bool StartMasterSession(std::string *msg, bool idempotent) override;
  bool StopMasterSession(std::string *msg, bool idempotent) override;

  // on success return name of new file in new_file.
  bool FlushLogs(std::string *new_file) override;

  // on success return name of oldest kept file in oldest_file.
  bool PurgeLogs(std::string *oldest_file) override;

  // Maintain set of allocated server id to make sure they are unique.
  // When allocating, try for up to timeout_ms. This is as that when
  // slave does STOP SLAVE/START SLAVE it takes a short time before
  // the slave deregisters. So that START SLAVE can get there faster.
  bool AllocServerId(uint32_t server_id, absl::Duration timeout) override;
  void FreeServerId(uint32_t server_id) override;

  const Uuid &GetUuid() const override { return uuid_; }

  std::string GetServerName() const override;

  const file::Factory &GetFileFactory() const override;

  mysql::MasterSession &GetMasterSession() const override {
    return *master_session_;
  }

  mysql::SlaveSessionFactory &GetSlaveSessionFactory() const override {
    return *slave_factory_;
  }

 private:
  friend class Manager;
  std::unique_ptr<Binlog> binlog_;
  mysql::ServerPort* port_;
  std::unique_ptr<ThreadPoolExecutor> pool_;
  std::unique_ptr<mysql::SlaveSessionFactory> slave_factory_;
  std::unique_ptr<Listener> listener_;
  std::unique_ptr<ManagementSession> manager_session_;
  std::unique_ptr<mysql::MasterSession> master_session_;
  std::unique_ptr<PurgeThread> purge_thread_;

  absl::Mutex server_id_mutex_;
  absl::flat_hash_set<uint32_t> allocated_server_ids_;


  absl::Mutex shutdown_mutex_;
  bool shutdown_requested_ GUARDED_BY(shutdown_mutex_) = false;
  Uuid uuid_;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_RIPPLED_H
