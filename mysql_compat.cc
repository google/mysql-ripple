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

#include "mysql_compat.h"

#include "gtid.h"

namespace mysql_ripple {

namespace mysql {

namespace compat {

// Convert a MariaDB GTIDStartPosition into a MySQL GTIDSet.
bool Convert(const GTIDStartPosition& src, GTIDSet *dst) {
  for (const GTID& gtid : src.gtids) {
    GTIDSet::Interval interval;
    interval.start = 1;
    interval.end = gtid.seq_no + 1;
    GTIDSet::GTIDInterval gtidinterval;
    gtidinterval.uuid = gtid.server_id.uuid;
    gtidinterval.intervals.push_back(interval);
    dst->AddInterval(gtidinterval);
  }

  return true;
}

bool Convert(const GTIDSet& src, GTIDStartPosition *dst) {
  for (const GTIDSet::GTIDInterval& interval : src.gtid_intervals_) {
    GTID gtid;
    gtid.server_id.uuid = interval.uuid;
    gtid.seq_no = interval.intervals.back().end - 1;
    dst->gtids.push_back(gtid);
  }

  return true;
}

bool Convert(const GTIDList& src, GTIDStartPosition *dst) {
  dst->Reset();
  for (const GTIDList::GTIDStream& stream : src.streams_) {
    GTID gtid;
    gtid.server_id = stream.server_id;
    gtid.domain_id = stream.domain_id;
    gtid.seq_no = stream.intervals.back().end - 1;
    if (!dst->Update(gtid))
      return false;
  }

  return true;
}

bool Convert(const GTIDList& src, GTIDSet *dst) {
  dst->Clear();
  for (const GTIDList::GTIDStream& stream : src.streams_) {
    GTIDSet::GTIDInterval gtid_interval;
    if (stream.server_id.uuid.empty()) {
      return false;
    }
    gtid_interval.uuid = stream.server_id.uuid;
    gtid_interval.intervals = stream.intervals;
    dst->gtid_intervals_.push_back(gtid_interval);
  }

  return true;
}

}  // namespace compat

}  // namespace mysql

}  // namespace mysql_ripple
