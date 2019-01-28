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

#ifndef MYSQL_RIPPLE_EXECUTOR_H
#define MYSQL_RIPPLE_EXECUTOR_H

#include <pthread.h>

#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace mysql_ripple {

class RunnableInterface {
 public:
  virtual ~RunnableInterface() {}
  virtual void Run() = 0;

  // request Runnable to Stop(), non-blocking
  virtual void Stop() = 0;

  // this is is the last call from ThreadPool
  virtual void Unref() = 0;
};

class ThreadPoolExecutor {
 public:
  ThreadPoolExecutor();
  virtual ~ThreadPoolExecutor();

  virtual bool Execute(RunnableInterface *runnable);

  virtual void Stop();

  virtual void WaitStopped();

  // A managed thread.
  struct Thread {
    pthread_t thread_;
    ThreadPoolExecutor *executor_;
    RunnableInterface *runnable_;
    void Run();
  };
 private:
  absl::Mutex mutex_;
  absl::flat_hash_map<pthread_t, Thread *> running_threads_;
  absl::flat_hash_map<pthread_t, Thread *> terminated_threads_;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_EXECUTOR_H
