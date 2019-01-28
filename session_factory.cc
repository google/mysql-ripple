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

#include "session_factory.h"

#include "logging.h"
#include "monitoring.h"
#include "mysql_server_connection.h"
#include "mysql_slave_session.h"

namespace mysql_ripple {

class Rippled;

namespace mysql {

SlaveSessionFactory::SlaveSessionFactory(
    SlaveSession::RippledInterface *rippled,
    BinlogReader::BinlogInterface *binlog, ThreadPoolExecutor *executor)
    : rippled_(rippled),
      binlog_(binlog),
      executor_(executor),
      connection_status_trigger_([this]() { SetConnectionStatusMetrics(); }) {}

SlaveSessionFactory::~SlaveSessionFactory() {}

bool SlaveSessionFactory::NewSession(Connection *connection) {
  if (connection->connection_type() != Connection::MYSQL_SERVER_CONNECTION) {
    LOG(ERROR) << "Incorrect type in SlaveSessionFactory::NewSession()"
               << ", got: " << std::to_string(connection->connection_type())
               << ", expected: "
               << std::to_string(Connection::MYSQL_SERVER_CONNECTION);
    return false;
  }

  ServerConnection *con = reinterpret_cast<ServerConnection *>(connection);
  SlaveSession *session = new SlaveSession(rippled_, con, binlog_, this);

  {
    absl::MutexLock lock(&mutex_);
    active_slaves_.insert(session);
    monitoring::num_slaves->Set(active_slaves_.size());
  }

  executor_->Execute(session);

  return true;
}

void SlaveSessionFactory::SetConnectionStatusMetrics() {
  absl::MutexLock lock(&mutex_);
  for (SlaveSession *slave : active_slaves_)
    slave->SetConnectionStatusMetrics();
}

void SlaveSessionFactory::EndSession(SlaveSession *session) {
  absl::MutexLock lock(&mutex_);
  if (active_slaves_.find(session) != active_slaves_.end()) {
    active_slaves_.erase(session);
    monitoring::num_slaves->Set(active_slaves_.size());
  } else {
    LOG(ERROR) << "Could not find slave session in collection of "
               << "active slaves";
  }
}

void SlaveSessionFactory::IterateSessions(SessionIteratorFn *iterator,
                                          void *cookie) {
  absl::MutexLock lock(&mutex_);
  for (SlaveSession *slave : active_slaves_) {
    iterator(slave, cookie);
  }
}

}  // namespace mysql

}  // namespace mysql_ripple
