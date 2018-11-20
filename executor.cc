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

#include "executor.h"

#include <cstdlib>

#include "absl/time/time.h"

extern "C" void *ThreadPoolExecutorRun_C(void *arg) {
  reinterpret_cast<mysql_ripple::ThreadPoolExecutor::Thread*>(arg)->Run();
  return nullptr;
}

namespace mysql_ripple {

ThreadPoolExecutor::ThreadPoolExecutor() {
}

ThreadPoolExecutor::~ThreadPoolExecutor() {
  Stop();
  WaitStopped();
}

bool ThreadPoolExecutor::Execute(RunnableInterface *runnable) {
  Thread *thr = new Thread();
  thr->executor_ = this;
  thr->runnable_ = runnable;
  absl::MutexLock lock(&mutex_);
  if (!pthread_create(&thr->thread_, nullptr, ThreadPoolExecutorRun_C, thr)) {
    running_threads_.emplace(thr->thread_, thr);
    return true;
  }
  return false;
}

void ThreadPoolExecutor::Stop() {
  absl::MutexLock lock(&mutex_);
  for (auto& thread : running_threads_) {
    Thread *obj = thread.second;
    obj->runnable_->Stop();
  }
}

void ThreadPoolExecutor::WaitStopped() {
  Stop();

  absl::MutexLock lock(&mutex_);
  auto check = [this]() { return running_threads_.empty(); };
  mutex_.Await(absl::Condition(&check));

  for (auto& thread : terminated_threads_) {
    void* retval;
    pthread_t id = thread.first;
    Thread *obj = thread.second;
    pthread_join(id, &retval);
    delete obj;
  }
  terminated_threads_.clear();
}

void ThreadPoolExecutor::Thread::Run() {
  runnable_->Run();
  absl::MutexLock lock(&executor_->mutex_);
  runnable_->Unref();
  runnable_ = nullptr;
  executor_->running_threads_.erase(thread_);
  executor_->terminated_threads_.emplace(thread_, this);
}

}  // namespace mysql_ripple
