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

#ifndef MYSQL_RIPPLE_SESSION_FACTORY_H
#define MYSQL_RIPPLE_SESSION_FACTORY_H

#include <set>

#include "binlog.h"
#include "connection.h"
#include "executor.h"
#include "monitoring.h"
#include "mysql_slave_session.h"

namespace mysql_ripple {

// Handle a connection.
class SessionFactory {
 public:
  virtual ~SessionFactory() {}
  virtual bool NewSession(Connection *connection) = 0;
};

namespace mysql {

class SlaveSession;

typedef void (SessionIteratorFn)(SlaveSession *session, void* cookie);

// This is a session factory, that accepts mysql slaves and runs
// them in one thread per session using the ThreadPoolExecutor.
class SlaveSessionFactory : public SessionFactory,
                            public SlaveSession::FactoryInterface {
 public:
  explicit SlaveSessionFactory(SlaveSession::RippledInterface *rippled,
                               BinlogReader::BinlogInterface *binlog,
                               ThreadPoolExecutor *executor);
  ~SlaveSessionFactory() override;

  bool NewSession(Connection *connection) override;
  void EndSession(SlaveSession *session) override;
  void IterateSessions(SessionIteratorFn *iterator, void *cookie);

 private:
  SlaveSession::RippledInterface *rippled_;
  BinlogReader::BinlogInterface *binlog_;
  ThreadPoolExecutor *executor_;

  std::set<SlaveSession*> active_slaves_;
  absl::Mutex mutex_;

  monitoring::CallbackTrigger connection_status_trigger_;
  void SetConnectionStatusMetrics();

  SlaveSessionFactory(SlaveSessionFactory&&) = delete;
  SlaveSessionFactory(const SlaveSessionFactory&) = delete;
  SlaveSessionFactory& operator=(SlaveSessionFactory&&) = delete;
  SlaveSessionFactory& operator=(const SlaveSessionFactory&) = delete;
};

}  // namespace mysql

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_SESSION_FACTORY_H
