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

#include "mysql_constants.h"

namespace mysql_ripple {

namespace constants {

std::string ToString(EventType type) {
  switch (type) {
    case ET_QUERY:
      return "Query";
    case ET_STOP:
      return "Stop";
    case ET_ROTATE:
      return "Rotate";
    case ET_FORMAT_DESCRIPTION:
      return "Format_desc";
    case ET_XID:
      return "Xid";
    case ET_INCIDENT:
      return "Incident";
    case ET_HEARTBEAT:
      return "Heartbeat";
    case ET_GTID_MYSQL:
      return "Gtid";
    case ET_PREVIOUS_GTIDS_MYSQL:
      return "Previous_gtids";
    case ET_GTID_MARIADB:
      return "Gtid";
    case ET_GTID_LIST_MARIADB:
      return "Gtid_list";
    case ET_BINLOG_CHECKPOINT:
      return "Binlog_checkpoint";
    case ET_START_ENCRYPTION:
      return "Start_encryption";
    default:
      break;
  }
  return
      std::string("Unknown event: ") + std::to_string(static_cast<int>(type));
}

}  // namespace constants

}  // namespace mysql_ripple
