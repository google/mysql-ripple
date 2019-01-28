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

#ifndef MYSQL_RIPPLE_BINLOG_POSITION_H
#define MYSQL_RIPPLE_BINLOG_POSITION_H

#include <string>

#include "file_position.h"
#include "gtid.h"
#include "log_event.h"

namespace mysql_ripple {

// This class represents a complete binlog position.
struct BinlogPosition {
  BinlogPosition() : group_state(NO_GROUP) {}

  // Init position.
  void Init(const std::string& filename,
            const GTIDList& start_pos,
            const FilePosition& master_pos) {
    latest_event_start_position.Init(filename);
    latest_event_end_position = latest_event_start_position;
    latest_completed_gtid_position = latest_event_start_position;

    latest_master_position = master_pos;
    next_master_position = master_pos;
    latest_start_gtid.Reset();
    latest_completed_gtid.Reset();
    gtid_start_position = start_pos;
    gtid_purged.Reset();
    group_state = NO_GROUP;
    own_format.Reset();
    master_format.Reset();
    master_server_id.reset();
  }

  // Reset position.
  void Reset() {
    own_format.Reset();
    master_format.Reset();
    master_server_id.reset();
    latest_event_start_position.Reset();
    latest_event_end_position.Reset();
    latest_completed_gtid_position.Reset();
    latest_master_position.Reset();
    next_master_position.Reset();
    latest_start_gtid.Reset();
    latest_completed_gtid.Reset();
    gtid_start_position.Reset();
    gtid_purged.Reset();
    group_state = NO_GROUP;
  }

  // A new file has been opened
  bool OpenFile(const std::string& filename, off_t offset) {
    if (group_state != NO_GROUP)
      return false;

    // No events can span two files
    latest_event_start_position.filename = filename;
    latest_event_start_position.offset = offset;
    latest_event_end_position = latest_event_start_position;

    // No GTID can span two files
    latest_completed_gtid_position = latest_event_start_position;
    return true;
  }

  // Format descriptor (version) of ripple producing this binlog file.
  FormatDescriptorEvent own_format;

  // Server Id of mysqld creating events for this binlog file.
  ServerId master_server_id;

  // Format descriptor for mysqld used to create events in this binlog file.
  FormatDescriptorEvent master_format;

  // File position for start of last event.
  FilePosition latest_event_start_position;

  // File position for end of last event.
  FilePosition latest_event_end_position;

  // File position for end of last GTID that has been completed.
  FilePosition latest_completed_gtid_position;

  // File position of master corresponding to end_position_last_event
  FilePosition latest_master_position;

  // File position of next master event
  FilePosition next_master_position;

  // File position of master corresponding to latest_completed_file_position
  FilePosition latest_completed_gtid_master_position;

  // Latest GTID that has been started.
  GTID latest_start_gtid;

  // Latest GTID that has been completed.
  GTID latest_completed_gtid;

  // Position to to restart from (if needed).
  // This is which gtid has already been executed.
  GTIDList gtid_start_position;

  // GTID purged, i.e set of gtids not present in binlog.
  GTIDList gtid_purged;

  enum GroupState {
    NO_GROUP,        // Not in a group, publish end pos after this event
    IN_TRANSACTION,  // Publish end pos after XID
    STANDALONE,      // Publish end pos after subsequent event
    END_OF_GROUP     // Publish end pos after this event...
  };
  GroupState group_state;

  // Update binlog position with event
  // return -1 error
  //         1 OK - end position updated
  //         0 OK - end position not updated
  int Update(RawLogEventData event, off_t end_offset);

  // Is there an transaction ongoing.
  bool InTransaction() const;

  // Human readable version, for logging/debugging.
  std::string ToString() const;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_BINLOG_POSITION_H
