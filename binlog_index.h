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

#ifndef MYSQL_RIPPLE_BINLOG_INDEX_H
#define MYSQL_RIPPLE_BINLOG_INDEX_H

#include <stdio.h>

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "binlog_position.h"
#include "file.h"
#include "file_util.h"
#include "gtid.h"

namespace mysql_ripple {

// This is a class that is used during recovery
// - to validates that a file is a valid binlog file
// - to remove files that are marked as purged
class BinlogRecoveryHandlerInterface {
 public:
  virtual ~BinlogRecoveryHandlerInterface() {}

  // Validate that filename points to a binlog file
  virtual file_util::OpenResultCode Validate(absl::string_view filename) = 0;

  // Remove a binlog file.
  virtual bool Remove(absl::string_view filename) = 0;

  // Get file size of a binlog file.
  virtual size_t GetFileSize(absl::string_view filename) = 0;
};

class BinlogReader;  // forward declaration to avoid circular includes.

// This class represents a binlog index.
// I.e a list of binlog files with start/last GTID.
//
// Thread safety of this class works as follows:
// 1) Create(), Recover(), Close() is NOT thread safe
//
// 2) One thread (writer) may call NewFile()/CloseFile()
// 3) Any number threads (readers) may call GetEntry(), GetCurrentEntry()
//    GetNextEntry()
class BinlogIndex {
 public:
  explicit BinlogIndex(const char* directory, const file::Factory& ff);
  virtual ~BinlogIndex();

  // Set/get basename.
  void SetBasename(absl::string_view basename);
  const std::string& GetBasename() const;

  // Create binlog index and open if non-exists.
  // If a binlog index already exists return false.
  virtual bool Create();

  // Open binlog index and recover/discard unfinished entries.
  // return 0 - no state found on disk
  //        1 - state recovered
  //       -1 - an error/inconsistency
  virtual int Recover(BinlogRecoveryHandlerInterface *handler);

  // Check if binlog index is open.
  virtual bool IsOpen();

  // Close an opened binlog index.
  virtual bool Close();

  // An index entry (line).
  struct Entry {
    Entry() { is_closed = is_purged = false; }

    // Filename
    std::string filename;

    // Highest GTID not present in this file.
    // I.e set of gtid that has been executed prior to this file.
    GTIDList start_position;

    // Master position of first event in this file.
    FilePosition start_master_position;

    // Last GTID in this file.
    // I.e set of gtid that has been executed at end of this file.
    GTIDList last_position;

    // Next master position of last event in this file.
    FilePosition last_next_master_position;

    // Is entry marked purged.
    bool is_purged;

    // Is current entry "closed".
    bool is_closed;

    // Size of file, only for closed files.
    // Not saved in index, but recreated during startup.
    size_t file_size;

    bool IsEmpty() const {
      return
          filename.empty() &&
          start_position.IsEmpty() &&
          start_master_position.IsEmpty() &&
          last_position.IsEmpty() &&
          last_next_master_position.IsEmpty();
    }

    void Reset() {
      is_purged = false;
      is_closed = false;
      filename.clear();
      start_position.Reset();
      start_master_position.Reset();
      last_position.Reset();
      last_next_master_position.Reset();
    }

    std::string ToString() const;
    bool Parse(absl::string_view line);
    std::string FormatHead() const;
    std::string FormatTail() const;
  };

  // Add a new entry to the index.
  virtual bool NewEntry(const GTIDList& start_position,
                        const FilePosition& master_position);

  // End current entry.
  virtual bool CloseEntry(const GTIDList& last_position,
                          const FilePosition& last_next_master_position,
                          size_t file_size);

  // Get oldest file needed for start_position.
  // Return false on failure, and then populates message with reason.
  virtual bool GetEntry(const GTIDList& start_pos, Entry *dst,
                        std::string *message = nullptr) const;

  // Get next file.
  virtual bool GetNextEntry(absl::string_view filename, Entry* dst) const;

  // Get oldest entry.
  // return empty Entry if no files are present in index.
  virtual Entry GetOldestEntry() const;

  // Get "current" entry. This is the last entry created with NewFile.
  // Entry can have empty last position (if CloseFile has not been called).
  virtual Entry GetCurrentEntry() const;

  // Get name of index file.
  virtual std::string GetIndexFilename() const;

  // Get total size of all binlogs in index
  virtual size_t GetTotalSize() const;

  // Mark entry as purged.
  // This will only rewrite the index file adding the purge flag to entry.
  // This is split from the purge so that the potentially slower Purge-call
  // can be made without holding external mutexes.
  // NOTE: entry is redundant but used to prevent race-conditions.
  virtual bool MarkFirstEntryPurged(const Entry& entry);

  // Purge the entry.
  // This removes entry from the index file (by rewriting the index).
  // NOTE: entry is redundant but used to prevent race-conditions.
  virtual bool PurgeFirstEntry(const Entry& entry);

  // MarkAndPurge last entry in index.
  // This removes entry from the index file (by rewriting the index).
  // This method is used during recovery when a binlog file
  // without any GTIDs is found.
  // NOTE: entry is redundant but used to prevent race-conditions.
  virtual bool MarkAndPurgeLastEntry(const Entry& entry);

 private:
  // directory
  std::string directory_;

  // basename
  std::string basename_;

  // The file factory.
  const file::Factory& ff_;

  // The index file.
  file::AppendOnlyFile* index_file_;

  // True iff the last write to the index file has just a head and no tail.
  bool last_file_line_is_open_;

  // Mutex protecting index file.
  // NewEntry and Purge can run concurrently and this mutex
  // make sure that only one of them writes to file at a time.
  mutable absl::Mutex file_mutex_;

  // Mutex protecting index_entries_
  // (which is only member variable that can be accessed concurrently
  // if using class according to API)
  mutable absl::Mutex entries_mutex_;

  // The index entries.
  std::vector<Entry> index_entries_;

  BinlogIndex(BinlogIndex&&) = delete;
  BinlogIndex(const BinlogIndex&) = delete;
  BinlogIndex& operator=(BinlogIndex&&) = delete;
  BinlogIndex& operator=(const BinlogIndex&) = delete;

  // Get next filename.
  std::string GetNextFilename(absl::string_view filename) const;

  // This function writes content of entries to the binlog index.
  // First it writes it to a temporary file, and then it renames that file
  // to overwrite the old.
  bool RewriteIndex(const std::vector<Entry>& entries);
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_BINLOG_H
