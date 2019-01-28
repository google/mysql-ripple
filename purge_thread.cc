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

#include "purge_thread.h"

#include "flags.h"
#include "logging.h"

namespace mysql_ripple {

// Try purge once every 3 minutes.
static const absl::Duration kCheckTime = absl::Minutes(3);

PurgeThread::PurgeThread(Binlog *binlog)
    : ThreadedSession(Session::PurgeThread),
      binlog_(binlog) {
}

PurgeThread::~PurgeThread() {
}

void* PurgeThread::Run() {
  while (!ShouldStop()) {
    std::string oldest_file;
    if (FLAGS_ripple_purge_expire_logs_days > 0) {
      // We consider a day to be 24 hours, even though it's not true in the
      // face of leap seconds, daylight saving time, etc.
      absl::Duration ttl =
          absl::Hours(FLAGS_ripple_purge_expire_logs_days * 24);
      binlog_->PurgeLogsBefore(absl::Now() - ttl, &oldest_file);
    }

    if (FLAGS_ripple_purge_logs_keep_size > 0) {
      binlog_->PurgeLogsKeepSize(FLAGS_ripple_purge_logs_keep_size,
                                 &oldest_file);
    }

    // Wait 3 minutes before checking again
    WaitState(Session::STOPPING, kCheckTime);
  }

  return nullptr;
}

}  // namespace mysql_ripple
