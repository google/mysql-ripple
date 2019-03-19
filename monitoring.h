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

#ifndef MYSQL_RIPPLE_MONITORING_H
#define MYSQL_RIPPLE_MONITORING_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace monitoring {
  void Initialize();

  // TODO(pivanof): Add some meaningful implementations for these classes.

  template <typename... Fields>
  class Counter {
   public:
    void Increment(Fields... fields) {
      // Not implemented yet
    }
  };

  template <typename T, typename... Fields>
  class CallbackMetric {
   public:
    void Set(const T& value, Fields... fields) {
      // Not implemented yet
    }
  };

  class CallbackTrigger {
   public:
    class Options {};

    CallbackTrigger(std::function<void()> functor) {
      // Not implemented yet
    }
  };

  template <typename T, typename... Fields>
  class Metric {
   public:
    void Set(const T& value, Fields... fields) {
      // Not implemented yet
    }
  };

  // The following metrics refer to the connection to the master:
  extern CallbackMetric<std::string>* master_connection_status;
  extern CallbackMetric<std::string>* last_master_connect_error;
  extern CallbackMetric<uint64_t>* time_since_master_last_connected;
  extern Metric<bool>* rippled_active;
  extern Metric<uint64_t>* bytes_sent_to_master;
  extern Metric<uint64_t>* bytes_received_from_master;

  // The following metrics refer to the connection to the slave(s):
  extern Metric<uint32_t, std::string>* slave_current_event_timestamp;
  extern CallbackMetric<std::string, std::string>* slave_connection_status;
  extern CallbackMetric<std::string, std::string>* last_slave_connect_error;
  extern Metric<uint>* num_slaves;
  extern Metric<uint64_t, std::string>* bytes_sent_to_slave;
  extern Metric<uint64_t, std::string>* bytes_received_from_slave;

  // Binlog metrics:
  extern Counter<std::string>* rippled_binlog_error;
  extern Metric<uint32_t>* binlog_last_event_timestamp;
  extern Metric<uint64_t>* binlog_last_event_received;

  // Error messages used with the binlog error counter:
  // File errors:
  const char ERROR_CREATE_FILE[] = "Failed to create file.";
  const char ERROR_FLUSH_FILE[] = "Failed to flush file.";
  const char ERROR_OPEN_FILE[] = "Failed to open file.";
  const char ERROR_READ_FILE[] = "Failed to read file.";
  const char ERROR_RENAME_FILE[] = "Failed to rename file.";
  const char ERROR_SEEK_FILE[] = "Failed to seek in file.";
  const char ERROR_STAT_FILE[] = "Failed to stat file.";
  const char ERROR_TRUNCATE_FILE[] = "Failed to truncate file.";
  const char ERROR_WRITE_FILE[] = "Failed to write file.";
  const char ERROR_UNLINK_FILE[] = "Failed to unlink file.";
  const char ERROR_CHATTR_FILE[] = "Failed to change file attributes.";

  // Binlog event parsing/validating/writing errors
  const char ERROR_PARSE_EVENT[] = "Failed to parse event.";
  const char ERROR_PARSE_FD[] = "Failed to parse FormatDescriptor.";
  const char ERROR_PARSE_GTID[] = "Failed to parse GTIDEvent.";
  const char ERROR_PARSE_QUERY[] = "Failed to parse QueryEvent.";
  const char ERROR_PARSE_XID[] = "Failed to parse XIDEvent.";
  const char ERROR_WRITE_EVENT[] = "Failed to write event.";
  const char ERROR_WRITE_FD[] = "Failed to write FormatDescriptor.";
  const char ERROR_VALIDATE_EVENT[] = "Failed to validate event.";

  // Position errors
  const char ERROR_APPLY_FD[] = "Failed to apply FormatDescriptor.";
  const char ERROR_GTID_NOT_VALID[] =
    "Received GTIDEvent is not a valid successor.";
  const char ERROR_INCORRECT_GROUP_STATE[] =
    "Incorrect group state when receiving event.";
  const char ERROR_UPDATE_BINLOG_POS[] = "Failed to update binlog position.";
  const char ERROR_UPDATE_START_POS[] =
    "Failed to update binlog start position.";

  // Rollback errors
  const char ERROR_ROLLBACK[] = "Failed to rollback after connection closed.";

  // Index errors
  const char ERROR_FAILED_REWRITE_INDEX[] = "Failed to rewrite index.";
  const char ERROR_FAILED_READ_INDEX[] = "Failed to read index.";
  const char ERROR_INCONSISTENT_INDEX[] = "Binlog index found inconsistent.";
  const char ERROR_INVALID_MAGIC[] =
    "Binlog file has missing or invalid magic number.";
  const char ERROR_MARK_ENTRY_AS_PURGED[] = "Failed to mark entry as purged.";
  const char ERROR_MARK_FILE_AS_PURGED[] = "Failed to mark file as purged.";
  const char ERROR_OLD_FILE_NOT_CLOSED[] =
    "Creating new binlog file when last not closed.";
  const char ERROR_PURGE_FILE_FROM_INDEX[] = "Failed to purge file from index.";
  const char ERROR_WRITE_INDEX_ENTRY[] =
    "Failed to write entry to binlog index.";

  // Binlog Reader Errors:
  const char ERROR_BINLOG_FILE_NOT_FOUND[] =
    "Expected binlog file does not exist.";
  const char ERROR_READ_EVENT[] = "Failed to read event.";
  const char ERROR_READER_SEEK[] = "Failed to seek event in binlog.";
  const char ERROR_READER_SWITCH_FILE[] =
    "BinlogReader failed to switch file.";

  // Encryptor errors
  const char ERROR_DECRYPT[] = "Failed to decrypt event.";
  const char ERROR_ENCRYPT[] = "Failed to encrypt event.";
  const char ERROR_GET_KEY_BYTES[] = "Failed to GetKeyBytes.";
  const char ERROR_INIT_ENCRYPTOR[] = "Failed to initialize Encryptor.";
  const char ERROR_WRITE_CRYPT_INFO[] = "Failed to WriteCryptInfo.";

  }  // namespace monitoring

#endif  // MYSQL_RIPPLE_MONITORING_H
