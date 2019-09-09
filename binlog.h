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

#ifndef MYSQL_RIPPLE_BINLOG_H
#define MYSQL_RIPPLE_BINLOG_H

#include <sys/types.h>

#include <set>
#include <string>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "binlog_index.h"
#include "binlog_position.h"
#include "binlog_reader.h"
#include "encryption.h"
#include "file.h"
#include "gtid.h"
#include "log_event.h"
#include "mysql_client_connection.h"

namespace mysql_ripple {

// This class represents the binlog, i.e sequence of log events.
//
// Thread safety of this class works as follows:
// 1) Create(), Recover(), and Close() are NOT thread safe
//
// 2) One thread (writer) may call AddEvent/SwitchFile()
// 3) Any number threads (readers) may call GetBinlogPosition(),
//    WaitBinlogEndPosition(), GetNextFile(), GetPosition()
class Binlog : public BinlogReader::BinlogInterface {
 public:
  explicit Binlog(const char *directory, int64_t max_binlog_size,
                  const file::Factory &ff);
  virtual ~Binlog() ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Create a binlog file and open if non-exists.
  // If a binlog already exists return false.
  // Not thread safe.
  virtual bool Create() ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Create binlog with start position and open if non-exists.
  // This is used for -ripple_requested_start_gtid_position.
  // If a binlog already exists return false.
  // Not thread safe.
  virtual bool Create(const GTIDList &)
      ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Open binlog and recover/discard unfinished entries.
  // return 0 - no state found on disk
  //        1 - state recovered
  //       -1 - an error/inconsistency
  // Not thread safe.
  virtual int Recover() ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Close the binlog, if open.
  // Always returns true.
  // Thread safe.
  virtual bool Close() ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Check if binlog is open.
  // Thread safe.
  virtual bool IsOpen() const ABSL_LOCKS_EXCLUDED(file_mutex_);

  // Get binlog position.
  // Thread safe.
  virtual BinlogPosition GetBinlogPosition()
      ABSL_LOCKS_EXCLUDED(position_mutex_);

  // Wait for a file position other than pos (for BinlogReader)
  // and store current end position in *pos.
  //
  // truncate_counter - OUT return no of times binlog has been truncated.
  // Thread safe.
  bool WaitBinlogEndPosition(FilePosition *pos, int64_t *truncate_counter,
                             absl::Duration timeout) override
      ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Connection established.
  // Thread safe.
  virtual bool ConnectionEstablished(const mysql::ClientConnection*);

  // Add an event to binlog.
  // If wait is true, this method blocks until event has been written to disk.
  virtual bool AddEvent(RawLogEventData event, bool wait)
      ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Switch local binlog file.
  // Store name of new file in newfile.
  virtual bool SwitchFile(std::string *newfile)
      ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Connection closed.
  // Thread safe.
  virtual void ConnectionClosed(const mysql::ClientConnection *)
      ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Register/unregister a binlog reader.
  // This is used when purging logs so that we don't purge too far.
  // Note: callers shall NOT hold mutexes when calling (Un)RegisterReader or
  // we might deadlock due to locking mutexes in opposite order.
  void RegisterReader(BinlogReader *reader) override;
  void UnregisterReader(BinlogReader *reader) override;

  // Get next file (for BinlogReader).
  // pos is in/out argument.
  // Thread safe.
  bool GetNextFile(FilePosition *pos) const override;

  // Get approximate position for a GTIDList (for BinlogReader).
  // Uses binlog index.
  // Return false on failure, and then populates message with reason.
  // Thread safe.
  bool GetPosition(const GTIDList &pos, BinlogPosition *dst,
                   std::string *message) const override;

  // Get path for filename (aka add directory)
  std::string GetPath(absl::string_view filename) const override;

  // Remove a binlog file.
  bool Remove(absl::string_view filename) ABSL_LOCKS_EXCLUDED(file_mutex_);

  // Get size of binlog file.
  bool GetBinlogSize(absl::string_view filename, off_t *size) const override
      ABSL_LOCKS_EXCLUDED(file_mutex_);

  // "Stop" binlog.
  // Wake up all binlog readers waiting for more data.
  virtual void Stop() ABSL_LOCKS_EXCLUDED(position_mutex_);

  // Purge logs.
  // On success, store name of oldest kept file in oldest_file.
  virtual bool PurgeLogs(std::string *oldest_file)
      ABSL_LOCKS_EXCLUDED(position_mutex_);

  // Purge logs with st_mtime < before_time.
  // On success, store name of oldest kept file in oldest_file.
  virtual bool PurgeLogsBefore(absl::Time before_time, std::string *oldest_file)
      ABSL_LOCKS_EXCLUDED(position_mutex_);

  // Purge logs, keeping at least keep_size bytes.
  // On success, store name of oldest kept file in oldest_file.
  virtual bool PurgeLogsKeepSize(size_t keep_size, std::string *oldest_file)
      ABSL_LOCKS_EXCLUDED(position_mutex_);

  // Purge logs up until not including to_file.
  // On success, store the name of oldest kept file in oldest_file.
  // Note that purge might stop short of to_file since a slave
  // might be using an older file.
  virtual bool PurgeLogsUntil(absl::string_view to_file,
                              std::string *oldest_file)
      ABSL_LOCKS_EXCLUDED(position_mutex_);

 private:
  //
  bool stop_;

  // This mutex covers position_ (update/read/wait)
  absl::Mutex position_mutex_;

  // This mutex prevents concurrent access to binlog_file_
  mutable absl::Mutex file_mutex_ ABSL_ACQUIRED_AFTER(position_mutex_);

  // directory for binlog index+files
  const std::string directory_;

  // max binlog size
  const int64_t max_binlog_size_;

  // The file factory.
  const file::Factory &ff_;

  // The current binlog file.
  file::AppendOnlyFile *binlog_file_ ABSL_GUARDED_BY(file_mutex_)
      ABSL_PT_GUARDED_BY(file_mutex_);

  // The binlog index.
  BinlogIndex index_;

  // The binlog position.
  BinlogPosition position_ ABSL_GUARDED_BY(position_mutex_);

  // The file position of the last GTID that has been fully flushed to storage.
  FilePosition flushed_gtid_position_;

  // Currently connected master mysqld.
  const mysql::ClientConnection *current_master_connection_;

  // Binlog encryptor, encrypts events (one by one).
  std::unique_ptr<BinlogEncryptor> encryptor_;

  // No of times we truncated binlog,
  // used to prevent readers from reading unpublished data.
  int64_t truncate_counter_;

  // Mutex covering readers_
  // To avoid deadlocks, never hold any other locks when acquiring/releasing
  // purge_mutex_.
  absl::Mutex purge_mutex_;

  // set of binlog readers, iterated when purging binlog.
  std::set<BinlogReader*> readers_;

  // Check is filename IsSafeToPurge with all readers_
  bool IsSafeToPurgeLocked(absl::string_view filename) const;

  // Validate an event prior to writing it to local binlog.
  bool ValidateEvent(RawLogEventData event);

  // Create a new binlog file (and add it to binlog index).
  // On entry the binlog must not be open
  // On success, it will be open
  // On failure, it will not be open
  bool CreateNewFile(const GTIDList &start_pos, const FilePosition &master_pos)
      ABSL_LOCKS_EXCLUDED(file_mutex_, position_mutex_);

  // Create a new binlog file (and add it to binlog index).
  // Shall be called with file_mutex_ & position_mutex_ locked.
  // On entry the binlog must not be open
  // On success, it will be open
  // On failure, it will not be open
  bool CreateNewFileLocked(const GTIDList &start_pos,
                           const FilePosition &master_pos)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(file_mutex_, position_mutex_);

  // Write start encryption event (if encryption is enabled).
  bool WriteCryptInfo(file::AppendOnlyFile *file);

  // Write one format descriptor event.
  bool WriteFormatDescriptor(file::AppendOnlyFile *file,
                             const FormatDescriptorEvent *fd,
                             ServerId serverId);

  // Write format descriptor for mysqld (and optionally StartEncryption)
  // to start of binlog file.
  bool WriteMasterFormatDescriptor()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(file_mutex_, position_mutex_);

  // SwitchFile, shall be called with file_mutex_ & position_mutex_ locked.
  // On entry the binlog must be open
  // On return, a new binlog file will be open
  // Returns true iff finalizing the old file and marking it for archival were
  // successful.
  bool SwitchFileLocked()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(file_mutex_, position_mutex_);

  // Check if this event shall be written to disk.
  bool SkipWritingEvent(RawLogEventData event) const;

  // Write an event to binlog file.
  bool WriteEvent(RawLogEventData event, off_t *offset, bool wait)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(file_mutex_);

  // Rollback any started but not completed transactions (GTIDs)
  // by truncating the binlog file. Updates pos to reflect actions taken.
  // The truncated variable is set to TRUE if the binlog file was truncated.
  bool Rollback(BinlogPosition *pos, bool *truncated)
      ABSL_LOCKS_EXCLUDED(file_mutex_);

  // Finalize a binlog file, indicating it will not be written to anymore.
  bool Finalize(absl::string_view filename)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(file_mutex_);

  // Mark a binlog file for archiving, which will move it to cheaper storage.
  bool Archive(absl::string_view filename)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(file_mutex_);

  // Close an opened binlog.
  // On entry the file must be open and file_mutex_ must be held.
  // On return, it will be closed and file_mutex_ remains held.
  void CloseFileLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(file_mutex_)
      ABSL_SHARED_LOCKS_REQUIRED(position_mutex_);

  Binlog(Binlog&&) = delete;
  Binlog(const Binlog&) = delete;
  Binlog& operator=(Binlog&&) = delete;
  Binlog& operator=(const Binlog&) = delete;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_BINLOG_H
