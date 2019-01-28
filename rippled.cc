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

#include <unistd.h>
#include <signal.h>

#include <atomic>

#include "absl/time/time.h"
#include "file.h"
#include "flags.h"
#include "init.h"
#include "logging.h"
#include "monitoring.h"
#include "mysql_init.h"
#include "plugin.h"
#include "rippled.h"

// Global atomic flag that can be cleared by sighandler,
// and is polled in WaitShutdown.
// Note that when the flag is CLEARED, shutdown has been requested.
// SET is the normal state for this flag.
static std::atomic_flag keep_running = ATOMIC_FLAG_INIT;
// Rippled object cannot be destroyed because it has pointers to resources that
// can't be deleted. Thus we need to keep this global pointer to it.
mysql_ripple::Rippled *rippled = NULL;

void sighandler(int signo) {
  // clear the atomic flag
  // this value is polled in WaitShutdown
  keep_running.clear();
}

int main(int argc, char **argv) {
  mysql_ripple::Init(argc, argv);

  LOG(INFO) << "InitPlugins";
  if (!mysql_ripple::plugin::InitPlugins()) {
    exit(1);
  }

  monitoring::Initialize();

  keep_running.test_and_set();
  signal(SIGTERM, sighandler);

  int exit_code = 0;
  rippled = new mysql_ripple::Rippled();
  LOG(INFO) << "Setup";
  if (rippled->Setup()) {
    LOG(INFO) << "Start";
    rippled->Start();
    rippled->WaitShutdown();
  } else {
    LOG(ERROR) << "Rippled::Setup() failed!";
    exit_code = 1;
  }

  LOG(INFO) << "Teardown";
  rippled->Teardown();

  LOG(INFO) << "exit() with code " << exit_code;
  exit(exit_code);
}

namespace mysql_ripple {

Rippled::Rippled() : port_(nullptr) {}

Rippled::~Rippled() {
}

bool Rippled::Setup() {
  if (FLAGS_ripple_server_uuid.empty()) {
    uuid_.ConstructFromServerId(FLAGS_ripple_server_id);
  } else {
    if (!uuid_.Parse(FLAGS_ripple_server_uuid)) {
      LOG(FATAL) << "Failed to parse ripple_server_uuid: "
                 << FLAGS_ripple_server_uuid;
    }
  }

  binlog_.reset(new Binlog(FLAGS_ripple_datadir.c_str(),
                           FLAGS_ripple_max_binlog_size, GetFileFactory()));
  GTIDList start_pos;
  if (!FLAGS_ripple_requested_start_gtid_position.empty()) {
    CHECK(start_pos.Parse(FLAGS_ripple_requested_start_gtid_position));
  }
  switch (binlog_->Recover()) {
    case 0:
      LOG(INFO) << "No binlog found, creating new";
      CHECK(binlog_->Create(start_pos));
      break;
    case 1:
      LOG(INFO) << "Recovered binlog";
      if (!start_pos.IsEmpty() && !start_pos.Equal(
                binlog_->GetBinlogPosition().gtid_start_position)) {
        LOG(WARNING) << "Ignoring -ripple_requested_start_gtid_position "
                     << "in favor of local binlog.";
      }
      break;
    default:
      LOG(ERROR) << "Failed to recover binlog!";
      return false;
  }

  mysql::InitClientLibrary();

  port_ = mysql::ServerPortFactory::GetInstance(
      FLAGS_ripple_server_type,
      FLAGS_ripple_server_address,
      FLAGS_ripple_server_ports);

  if (port_ == nullptr) {
    return false;
  }

  if (!port_->Bind()) {
    return false;
  }
  if (!port_->Listen()) {
    return false;
  }

  pool_.reset(new ThreadPoolExecutor());
  slave_factory_.reset(new mysql::SlaveSessionFactory(this,
                                                      binlog_.get(),
                                                      pool_.get()));
  listener_.reset(new Listener(slave_factory_.get(), port_));
  master_session_.reset(new mysql::MasterSession(binlog_.get(), this));
  purge_thread_.reset(new PurgeThread(binlog_.get()));

  return true;
}

void Rippled::Teardown() {
  // First stop all objects.
  if (pool_.get() != nullptr)
    pool_->Stop();
  if (binlog_.get() != nullptr)
    binlog_->Stop();
  if (master_session_.get() != nullptr)
    master_session_->Stop();
  if (listener_.get() != nullptr)
    listener_->Stop();
  if (purge_thread_.get() != nullptr)
    purge_thread_->Stop();
  // Manager session does not need to be stopped because its run method will
  // return when the RPC server it borrows from the listener dies.

  // Then wait for them to stop.
  if (master_session_.get() != nullptr)
    master_session_->WaitState(Session::STOPPED, absl::Seconds(3));
  LOG(INFO) << "Stopped master session!";
  if (listener_.get() != nullptr)
    listener_->WaitState(Session::STOPPED, absl::Seconds(3));
  LOG(INFO) << "Stopped listener!";

  if (purge_thread_.get() != nullptr)
    purge_thread_->WaitState(Session::STOPPED, absl::Seconds(3));

  if (manager_session_.get() != nullptr) {
    LOG(INFO) << "Manager session still exists...";
    manager_session_->WaitState(Session::STOPPED, absl::Seconds(3));
    manager_session_->Shutdown(port_);
  }

  // Make sure all SlaveSessions have ended before destroying the
  // SlaveSessionFactory, since they are registered there.
  if (pool_.get() != nullptr)
    pool_->WaitStopped();

  if (port_ != nullptr)
    port_->Close();

  purge_thread_.reset(nullptr);
  manager_session_.reset(nullptr);
  master_session_.reset(nullptr);
  listener_.reset(nullptr);
  slave_factory_.reset(nullptr);
  pool_.reset(nullptr);
  binlog_.reset(nullptr);
  mysql::DeinitClientLibrary();
}

bool Rippled::Start() {
  if (manager_session_.get() != nullptr) {
    manager_session_->Start();
    manager_session_->WaitStarted();
  }

  listener_->Start();
  listener_->WaitStarted();

  if (!FLAGS_ripple_master_address.empty()) {
    // Only start master session if we have address to connect to.
    master_session_->Start();
    master_session_->WaitStarted();
  }
  purge_thread_->Start();

  return true;
}

void Rippled::Shutdown() {
  absl::MutexLock lock(&shutdown_mutex_);
  shutdown_requested_ = true;
}

void Rippled::WaitShutdown() {
  absl::MutexLock lock(&shutdown_mutex_);
  // use condition for SHUTDOWN case to avoid waiting 1s.
  while (!shutdown_requested_) {
    if (!keep_running.test_and_set())
      shutdown_requested_ = true;
    shutdown_mutex_.AwaitWithTimeout(absl::Condition(&shutdown_requested_),
                                     absl::Seconds(1));
  }
}

BinlogPosition Rippled::GetBinlogPosition() const {
  return binlog_->GetBinlogPosition();
}

bool Rippled::StartMasterSession(std::string *msg, bool idempotent) {
  if (idempotent) {
    if (master_session_->session_state() == Session::STARTED)
      return true;
    if (master_session_->session_state() == Session::STARTING) {
      master_session_->WaitStarted();
      return true;
    }
  }

  if (!master_session_->Start()) {
    msg->assign("Incorrect state!");
    return false;
  }

  master_session_->WaitStarted();
  return true;
}

bool Rippled::StopMasterSession(std::string *msg, bool idempotent) {
  if (idempotent) {
    if (master_session_->session_state() == Session::INITIAL)
      return true;
    if (master_session_->session_state() == Session::STOPPING ||
        master_session_->session_state() == Session::STOPPED)
      return master_session_->WaitState(
          Session::INITIAL, absl::InfiniteDuration()) == Session::INITIAL;
  }

  if (!master_session_->Stop()) {
    msg->assign("Incorrect state!");
    return false;
  }

  if (master_session_->Join()) {
    return true;
  }

  msg->assign("Failed to stop!");
  return false;
}

bool Rippled::FlushLogs(std::string *new_file) {
  return binlog_->SwitchFile(new_file);
}

bool Rippled::PurgeLogs(std::string *oldest_file) {
  return binlog_->PurgeLogs(oldest_file);
}

bool Rippled::AllocServerId(uint32_t server_id, absl::Duration timeout) {
  absl::MutexLock lock(&server_id_mutex_);
  auto check = [this, server_id]() {
    return !allocated_server_ids_.contains(server_id);
  };
  if (!server_id_mutex_.AwaitWithTimeout(absl::Condition(&check), timeout))
    return false;
  allocated_server_ids_.insert(server_id);
  return true;
}

void Rippled::FreeServerId(uint32_t server_id) {
  absl::MutexLock lock(&server_id_mutex_);
  allocated_server_ids_.erase(server_id);
}

std::string Rippled::GetServerName() const {
  return FLAGS_ripple_server_name;
}

const file::Factory &Rippled::GetFileFactory() const {
  return DEFAULT_FILE_FACTORY();
}

}  // namespace mysql_ripple
