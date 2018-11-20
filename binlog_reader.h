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

#ifndef MYSQL_RIPPLE_BINLOG_READER_H
#define MYSQL_RIPPLE_BINLOG_READER_H

#include <cstdio>
#include <vector>

#include "absl/strings/string_view.h"
#include "binlog_index.h"
#include "binlog_position.h"
#include "buffer.h"
#include "encryption.h"
#include "file.h"
#include "file_util.h"
#include "log_event.h"
#include "absl/synchronization/mutex.h"

namespace mysql_ripple {

// This class is used for reading the binlog.
class BinlogReader {
 public:
  // This is the interface used by BinlogReader to access the Binlog itself.
  class BinlogInterface : public BinlogEndPositionProviderInterface {
   public:
    virtual bool GetPosition(const GTIDList &start_pos, BinlogPosition *pos,
                             std::string *message) const = 0;
    virtual bool GetNextFile(FilePosition *pos) const = 0;
    virtual void RegisterReader(BinlogReader *reader) = 0;
    virtual void UnregisterReader(BinlogReader *reader) = 0;
    virtual std::string GetPath(absl::string_view filename) const = 0;
    virtual bool GetBinlogSize(absl::string_view filename,
                               off_t *size) const = 0;
  };

  explicit BinlogReader(const file::Factory &, BinlogInterface *,
                        BinlogEndPositionProviderInterface * = nullptr);
  virtual ~BinlogReader();

  // Open binlog starting in specific file.
  // This is used during binlog recovery...
  virtual bool Open(const BinlogIndex::Entry&);

  // Open binlog and seek to correct position.
  // This is used by SlaveSession to send events to a slave.
  // If there is an error, *message is populated with error message.
  virtual bool Open(GTIDList *pos, std::string *message);

  // Close binlog.
  virtual bool Close();

  // Read an event from binlog into *event.
  // returns - READ_ERROR on error.
  //         - READ_EOF if getting eof in middle of event
  //         - READ_OK and event with length == 0 on timeout
  //           (this can only happen at end of binlog)
  virtual file_util::ReadResultCode ReadEvent(RawLogEventData *event,
                                              absl::Duration timeout);

  // Get current binlog position of this reader.
  // This method is thread-safe and should/can be used for monitoring.
  // If Reader has not completed seeking, an empty position will be returned.
  virtual BinlogPosition GetBinlogPosition() const {
    absl::MutexLock lock(&mutex_);
    if (seek_completed_)
      return position_;
    return BinlogPosition();
  }

  // This method gets current binlog position regardless if seek is completed
  // or not. It is used by recovery code.
  virtual BinlogPosition GetBinlogPositionUnsafe() const {
    absl::MutexLock lock(&mutex_);
    return position_;
  }

  // Validate that filename points to a non-empty binlog file
  file_util::OpenResultCode Validate(absl::string_view filename);

  // Get copy of the binlog encryptor.
  // Used by Binlog::Recover() to restore same encryption state
  // as that when ripple was shutdown.
  BinlogEncryptor *CopyEncryptor() const { return encryptor_->Copy(); }

  // Check if it's safe to purge a file.
  bool IsSafeToPurge(absl::string_view filename) const;

  // If ReadEvent returns READ_EOF, one can use this method to see
  // how big current file is. This is used to truncate away half written
  // events at end of binlog.
  off_t GetEndOfFile() const { return end_of_file_; }

 private:
  mutable absl::Mutex mutex_;
  BinlogInterface *binlog_;
  BinlogEndPositionProviderInterface *binlog_endpos_;
  std::unique_ptr<BinlogEncryptor> encryptor_;
  const file::Factory &ff_;
  file::InputFile *binlog_file_;
  off_t end_of_file_;  // size of current binlog file
  int64_t truncate_counter_;  // has binlog been truncated.
  BinlogPosition position_;
  Buffer buffer_;

  bool seek_completed_;

  file_util::OpenResultCode OpenAndValidate(file::InputFile **file,
                                            absl::string_view filename);
  bool OpenFile();
  void CloseFile();
  bool SwitchFile();
  file_util::ReadResultCode Read(Buffer *dst);
  void SetCurrentFile(absl::string_view filename);
  void ReopenBinlogFile();

  // Seek to given position.
  // Modifies GTIDList and removes GTIDs that will not be
  // found by subsequent ReadEvent. GTIDs that *might* be found are
  // kept.
  // if Seek() failed *message is populated with error message.
  bool Seek(GTIDList *pos, std::string *message);
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_BINLOG_READER_H
