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

#include "session.h"

#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace mysql_ripple {

Session::Session(SessionType type)
    : session_type_(type), session_state_(INITIAL) {
}

Session::~Session() {
}

Session::SessionState Session::WaitState(SessionState state,
                                         absl::Duration timeout) {
  absl::MutexLock lock(&mutex_);
  return WaitStateLocked(state, timeout);
}

/**
 * Wait for specific state.
 * returns state when method exits.
 */
Session::SessionState Session::WaitStateLocked(SessionState state,
                                               absl::Duration timeout) {
  auto check = [this, state]() {
    return session_state_ == state ||
           !CheckValidTransition(session_state_, state);
  };
  mutex_.AwaitWithTimeout(absl::Condition(&check), timeout);
  return session_state_;
}

void Session::SetState(SessionState new_state) {
  absl::MutexLock lock(&mutex_);
  SetStateLocked(new_state);
}

void Session::SetStateLocked(SessionState new_state) {
  session_state_ = new_state;
}

bool Session::CheckValidTransition(SessionState from, SessionState to) {
  switch (to) {
    case INITIAL:
      return from == STOPPED;
    case STARTING:
      return from == INITIAL;
    case STARTED:
      return from == STARTING;
    case STOPPING:
      return from == STARTED;
    case STOPPED:
      return from == STARTED || from == STOPPING;
    case ERROR_STATUS:
      return false;  // can't wait for error
  }
}

ThreadedSession::ThreadedSession(SessionType type) : Session(type) {
}

ThreadedSession::~ThreadedSession() {
  Join();
}

bool ThreadedSession::Start() {
  bool result = false;
  absl::MutexLock lock(&mutex_);
  do {
    if (session_state_ != INITIAL)
      break;

    SetStateLocked(STARTING);
    pthread_create(&thread_, nullptr, Run_C, this);
    result = true;
  } while (0);

  return result;
}

void* Run_C(void *arg) {
  ThreadedSession *session = reinterpret_cast<ThreadedSession*>(arg);
  return session->RunImpl();
}

void* ThreadedSession::RunImpl() {
  void *result = nullptr;
  {
    absl::MutexLock lock(&mutex_);
    // Check if someone stopped us before we started
    if (session_state_ != STARTING)
      goto end;
    SetStateLocked(STARTED);
  }
  result = Run();

end:
  SetState(STOPPED);

  return result;
}

bool ThreadedSession::Stop() {
  absl::MutexLock lock(&mutex_);

  switch (session_state_) {
    case INITIAL:
      return false;
    case STARTING:
    case STARTED:
      SetStateLocked(STOPPING);
      return true;
    case STOPPING:
    case STOPPED:
      return true;
    case ERROR_STATUS:
      return false;
  }

  return false;
}

bool ThreadedSession::Join() {
  bool stop = Stop();
  absl::MutexLock lock(&mutex_);
  if (stop && WaitStateLocked(STOPPED, absl::InfiniteDuration()) == STOPPED) {
    void *retval;
    pthread_join(thread_, &retval);
    SetStateLocked(INITIAL);
    return true;
  }
  return false;
}

}  // namespace mysql_ripple
