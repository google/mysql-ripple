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

#ifndef MYSQL_RIPPLE_SESSION_H
#define MYSQL_RIPPLE_SESSION_H

#include "absl/synchronization/mutex.h"

namespace mysql_ripple {

// This class represents a session.
// I.e a connection from/to rippled.
class Session {
 public:
  enum SessionType {
    Listener,            // This is a thread waiting for connections
    MysqlMasterSession,  // This is a connection to a mysql master.
    MysqlSlaveSession,   // This is a slave connected to rippled.
    MgmSession,          // This is a monitoring/management connection
    PurgeThread
  };

  enum SessionState {
    ERROR_STATUS,        //
    INITIAL,             // Session is created
    STARTING,            // Session is starting
    STARTED,             // Session is started
    STOPPING,            // Session is stopping
    STOPPED              // Session is stopped but not joined
  };

  explicit Session(SessionType);
  virtual ~Session();

  SessionType session_type() const { return session_type_; }

  // Return current session state.
  SessionState session_state() const {
    absl::MutexLock lock(&mutex_);
    return session_state_;
  }

  // Return current session state, use if mutex is locked.
  SessionState SessionStateLocked() const {
    return session_state_;
  }

  // Wait for specific state.
  // returns state when method exits.
  SessionState WaitState(SessionState state, absl::Duration timeout);

  // Wait for session to enter state STARTED.
  void WaitStarted() {
    WaitState(STARTED, absl::InfiniteDuration());
  }

 private:
  const SessionType session_type_;
  bool CheckValidTransition(SessionState from, SessionState to);

 protected:
  void SetState(SessionState new_state);
  void SetStateLocked(SessionState new_state);
  SessionState WaitStateLocked(SessionState state, absl::Duration timeout);
  virtual bool ShouldStop() const { return session_state() != STARTED; }

  mutable absl::Mutex mutex_;
  SessionState session_state_;

  Session(Session&&) = delete;
  Session(const Session&) = delete;
  Session& operator=(Session&&) = delete;
  Session& operator=(const Session&) = delete;
};

// Function prototype for pthread start_routine
extern "C" void* Run_C(void *arg);

// This is a session that is ran by a thread.
class ThreadedSession : public Session {
  friend void* Run_C(void *arg);
 public:
  explicit ThreadedSession(SessionType);
  virtual ~ThreadedSession();

  virtual bool Start();
  virtual bool Stop();
  virtual bool Join();

 private:
  pthread_t thread_;
  void* RunImpl();

 protected:
  virtual void* Run() = 0;

  ThreadedSession(ThreadedSession&&) = delete;
  ThreadedSession(const ThreadedSession&) = delete;
  ThreadedSession& operator=(ThreadedSession&&) = delete;
  ThreadedSession& operator=(const ThreadedSession&) = delete;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_SESSION_H
