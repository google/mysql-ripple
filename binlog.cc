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

#include "binlog.h"

#include <sys/types.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "binlog_reader.h"
#include "file.h"
#include "flags.h"
#include "logging.h"
#include "monitoring.h"
#include "mysql_constants.h"

namespace mysql_ripple {

Binlog::Binlog(const char *directory, int64_t max_binlog_size,
               const file::Factory &ff)
    : stop_(false),
      directory_(absl::StrCat(absl::StripSuffix(directory, "/"), "/")),
      max_binlog_size_(max_binlog_size),
      ff_(ff),
      binlog_file_(nullptr),
      index_(directory, ff),
      encryptor_(
          BinlogEncryptorFactory::GetInstance(FLAGS_ripple_encryption_scheme)),
      truncate_counter_(0) {
  position_.own_format.SetToRipple(FLAGS_ripple_version_binlog.c_str());
}

Binlog::~Binlog() { Close(); }

// Create binlog and open if non-exists.
// If a binlog already exists return false.
bool Binlog::Create() {
  GTIDList start_pos;
  return Create(start_pos);
}

bool Binlog::Create(const GTIDList &start_pos) {
  if (!index_.Create()) {
    LOG(ERROR) << "Failed to create binlog index";
    return false;
  }

  FilePosition master_pos;
  if (!CreateNewFile(start_pos, master_pos)) {
    LOG(ERROR) << "Failed to create new binlog file";
    return false;
  }
  absl::MutexLock position_lock(&position_mutex_);
  position_.gtid_purged = start_pos;
  return true;
}

bool Binlog::CreateNewFile(const GTIDList& start_pos,
                           const FilePosition& master_pos) {
    absl::MutexLock position_lock(&position_mutex_);
    absl::MutexLock file_lock(&file_mutex_);
    return CreateNewFileLocked(start_pos, master_pos);
}

bool Binlog::CreateNewFileLocked(const GTIDList& start_pos,
                                 const FilePosition& master_pos) {
  CHECK(binlog_file_ == nullptr);
  // Write an entry to binlog index before creating the actual binlog file.
  // This is done to avoid "dangling" binlog files.
  // During recover we will scan the index and check if the binlog file
  // is present and if an index entry is found without the corresponding
  // binlog file, this is silently ignored. See Recover().
  if (!index_.NewEntry(start_pos, master_pos)) {
    LOG(ERROR) << "Failed to create new binlog index entry";
    return false;
  }

  BinlogIndex::Entry entry = index_.GetCurrentEntry();
  file::AppendOnlyFile *file;
  if (!ff_.Open(&file, GetPath(entry.filename), "a")) {
    LOG(ERROR) << "Failed to create new binlog file " << entry.filename
               << " - open failed!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_CREATE_FILE);
    return false;
  }

  absl::string_view header(constants::BINLOG_HEADER,
                           sizeof(constants::BINLOG_HEADER));
  if (!file->Write(header)) {
    LOG(ERROR) << "Failed to create new binlog file " << entry.filename
               << " - write of header failed!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_FILE);
    file->Close();
    return false;
  }

  mysql_ripple::ServerId server_id;
  server_id.assign(FLAGS_ripple_server_id);
  if (!WriteFormatDescriptor(file, &position_.own_format, server_id)) {
    LOG(ERROR) << "Failed to write own format descriptor!!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_FD);
    file->Close();
    return false;
  }

  if (!file->Flush()) {
    LOG(ERROR) << "Failed to create new binlog file " << entry.filename
               << " - flush failed!";
    monitoring::rippled_binlog_error->Increment(monitoring::ERROR_FLUSH_FILE);
    file->Close();
    return false;
  }

  // BUG: ignoring errors from Tell.
  int64_t offset;
  file->Tell(&offset);
  binlog_file_ = file;
  FilePosition pos(entry.filename, offset);
  position_.latest_event_end_position = pos;
  position_.latest_completed_gtid_position = pos;
  position_.gtid_start_position = start_pos;

  return true;
}

bool Binlog::WriteMasterFormatDescriptor() {
  // Write FD for mysqld (aka remote)
  mysql_ripple::ServerId server_id;
  server_id = position_.master_server_id;
  if (!WriteFormatDescriptor(binlog_file_,
                             &position_.master_format, server_id)) {
    LOG(ERROR) << "Failed to write master format descriptor!!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_FD);
    return false;
  }

  if (!encryptor_->Init()) {
    LOG(ERROR) << "Failed to init encryptor";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_INIT_ENCRYPTOR);
    return false;
  }

  // And then write StartEncryptionEvent
  if (!WriteCryptInfo(binlog_file_)) {
    LOG(ERROR) << "Failed to WriteCryptInfo";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_CRYPT_INFO);
    return false;
  }

  if (!binlog_file_->Flush()) {
    LOG(ERROR) << "Failed to write master format descriptor - flush failed";
    monitoring::rippled_binlog_error->Increment(monitoring::ERROR_FLUSH_FILE);
    return false;
  }

  // Update file positions
  FilePosition pos = position_.latest_event_end_position;
  // BUG: ignoring errors from Tell.
  int64_t offset;
  binlog_file_->Tell(&offset);
  pos.offset = offset;
  position_.latest_event_end_position = pos;
  position_.latest_completed_gtid_position = pos;
  flushed_gtid_position_ = pos;
  return true;
}

bool Binlog::WriteFormatDescriptor(file::AppendOnlyFile *file,
                                   const FormatDescriptorEvent *fd,
                                   ServerId serverId) {
  Buffer buf;
  LogEventHeader header;
  int64_t offset;
  // BUG: ignoring errors from Tell.
  file->Tell(&offset);
  memset(&header, 0, sizeof(header));
  header.type = constants::ET_FORMAT_DESCRIPTION;
  header.event_length = header.PackLength() + fd->PackLength();
  header.server_id = serverId.server_id;
  header.nextpos = offset + header.event_length;

  uint8_t *ptr = buf.Append(header.event_length);
  if (!header.SerializeToBuffer(ptr, header.PackLength())) {
    LOG(ERROR) << "Failed to serialize binlog event header";
    return false;
  }
  if (!fd->SerializeToBuffer(ptr + header.PackLength(),
                             header.event_length - header.PackLength())) {
    LOG(ERROR) << "Failed to serialize binlog format descriptor";
    return false;
  }

  if (!file->Write(buf)) {
    LOG(ERROR) << "Failed to write format descriptor to binlog";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_FILE);
    return false;
  }
  return true;
}

bool Binlog::WriteCryptInfo(file::AppendOnlyFile *file) {
  Buffer buf;
  LogEventHeader header;
  memset(&header, 0, sizeof(header));
  header.server_id = FLAGS_ripple_server_id;
  int64_t offset;
  file->Tell(&offset);
  header.nextpos = offset;
  switch (encryptor_->GetStartEncryptionEvent(header, &buf)) {
    case 0:
      // no encryption event needed
      return true;
    case 1:
      break;
    default:
      LOG(ERROR) << "Failed to GetStartEncryptionEvent()";
      return false;  // error
  }

  if (!file->Write(buf)) {
    LOG(ERROR) << "Failed to write crypt info to binlog";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_FILE);
    return false;
  }
  return true;
}

// This class is used during recovery so that binlog_reader get correct
// end position.
class BinlogRecoveryEndPosition : public BinlogEndPositionProviderInterface {
 public:
  virtual ~BinlogRecoveryEndPosition() {}

  // The filename/file-size of the last file in the binlog.
  FilePosition end_position;

  bool WaitBinlogEndPosition(FilePosition *pos,
                             int64_t *truncate_counter,
                             absl::Duration timeout) override {
    // NOTE:
    // 1) We don't need to consider timeout in this class as it's only used
    // during Recover where no new events are produced.
    // 2) Just set truncate counter to 0 as there are no slaves connected
    // during recovery.
    *truncate_counter = 0;
    bool retVal = !end_position.equal(*pos);
    *pos = end_position;
    return retVal;
  }
};

// This class is used during recovery to validate/remove binlog files.
class BinlogRecoveryHandler : public BinlogRecoveryHandlerInterface {
 public:
  BinlogRecoveryHandler(Binlog *binlog, BinlogReader *reader)
      : binlog_(binlog), binlog_reader_(reader) {}
  virtual ~BinlogRecoveryHandler() {}

  // Validate that filename points to a binlog file
  file_util::OpenResultCode Validate(absl::string_view filename) override {
    return binlog_reader_->Validate(filename);
  }

  // Remove a binlog file.
  bool Remove(absl::string_view filename) override {
    return binlog_->Remove(filename);
  }

  // Get file size of a binlog file.
  size_t GetFileSize(absl::string_view filename) override {
    off_t size;
    binlog_->GetBinlogSize(filename, &size);
    return size;
  }

 private:
  Binlog *binlog_;
  BinlogReader *binlog_reader_;
};

// Open binlog and recover/discard unfinished entries.
// return 0 - no state found on disk
//        1 - state recovered
//       -1 - an error/inconsistency
int Binlog::Recover() {
  LOG(INFO) << "Starting binlog recovery";
  BinlogRecoveryEndPosition recovery_end_pos;
  BinlogReader reader(ff_, this, &recovery_end_pos);
  BinlogRecoveryHandler recoveryHandler(this, &reader);
  switch (index_.Recover(&recoveryHandler)) {
    case 0:   // no data found
      return 0;
    case 1:   // ok
      break;
    default:  // error
      return -1;
  }

  bool new_file = false;
  BinlogIndex::Entry entry = index_.GetCurrentEntry();
  if (entry.IsEmpty() || !entry.last_position.IsEmpty()) {
    GTIDList start_pos = entry.last_position;
    if (entry.IsEmpty())
      LOG(INFO) << "Found empty index, creating new file";
    if (!entry.last_position.IsEmpty())
      LOG(INFO) << "Last file of binlog is closed, creating new file";
    if (!CreateNewFile(start_pos, entry.last_next_master_position)) {
      return -1;
    }
    Close();                           // close the file we just created
    entry = index_.GetCurrentEntry();  // and refetch the entry
    // remember that we created a fresh file so that we don't truncate/recurse
    new_file = true;
  }

  if (!reader.Open(entry)) {
    LOG(ERROR) << "Failed to open binlog file: " << entry.filename;
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_OPEN_FILE);
    // we should be able to open recovered entry.
    // if we can't that is an error.
    return -1;
  }

  // TODO(jonaso) : use gtid-index, and only scan forward from latest
  // index:ed gtid

  // Try to read to end of last file.
  LOG(INFO) << "Scanning binlog file: " << entry.filename;

  recovery_end_pos.end_position.filename = entry.filename;
  GetBinlogSize(entry.filename.c_str(), &recovery_end_pos.end_position.offset);

  // Read from last index position to end of file
  RawLogEventData ev;
  do {
    switch (reader.ReadEvent(&ev, absl::ZeroDuration())) {  // no wait
      case file_util::READ_OK:
        break;
      case file_util::READ_EOF:
        // It's ok to find EOF during recovery,
        ev.header.event_length = 0;
        break;
      case file_util::READ_ERROR:
        reader.Close();
        return -1;
    }
    if (ev.header.event_length == 0)
      break;
  } while (true);

  BinlogPosition pos = reader.GetBinlogPositionUnsafe();

  if (!new_file && pos.gtid_start_position.Equal(entry.start_position)) {
    // This file does not contain any GTIDs.
    // 1) Remove file.
    CHECK(Remove(entry.filename))
        << "Failed to remove empty binlog file: "
        << entry.filename;

    // 2) Then remove it from index.
    CHECK(index_.MarkAndPurgeLastEntry(entry))
        << "Failed to purge last entry from index!!";
    pos.master_format.Reset();
    reader.Close();
    // 3) And finally recurse.
    return Recover();
  } else {
    // NOTE: copy encryptor before closing reader as it resets encryptor
    // in Close(). But if it's a new file, the reader encryptor is empty.
    if (!new_file) {
      encryptor_.reset(reader.CopyEncryptor());
    }
    reader.Close();
  }

  pos.latest_event_end_position.offset = reader.GetEndOfFile();
  pos.gtid_purged = index_.GetOldestEntry().start_position;

  bool truncated;
  // Rollback to latest completed GTID.
  if (!Rollback(&pos, &truncated)) {
    return -1;
  }

  // Open last file
  const FilePosition& end = pos.latest_completed_gtid_position;
  Close();
  if (!ff_.Open(&binlog_file_, GetPath(end.filename), "a")) {
    return -1;
  }

  absl::MutexLock lock(&position_mutex_);
  position_ = pos;

  LOG(INFO) << "Binlog recovery complete\n"
            << "binlog file: " << end.filename
            << ", offset: " << end.offset
            << ", gtid: " << pos.gtid_start_position.ToString();

  return 1;
}

// Check if binlog is open.
bool Binlog::IsOpen() const {
  absl::ReaderMutexLock file_lock(&file_mutex_);
  return binlog_file_ != nullptr;
}

// Close an opened binlog.
bool Binlog::Close() {
  absl::MutexLock file_lock(&file_mutex_);
  if (binlog_file_ != nullptr)
    CloseFileLocked();
  return true;
}

// Close an opened binlog.
void Binlog::CloseFileLocked() {
  CHECK(binlog_file_ != nullptr);
  binlog_file_->Sync();
  binlog_file_->Close();
  binlog_file_ = nullptr;
  flushed_gtid_position_ = position_.latest_completed_gtid_position;
}

bool Binlog::GetPosition(const GTIDList &pos, BinlogPosition *dst,
                         std::string *message) const {
  BinlogIndex::Entry entry;
  if (!index_.GetEntry(pos, &entry, message)) {
    return false;
  }
  if (!pos.IsEmpty() && entry.last_position.Equal(pos)) {
    CHECK(index_.GetNextEntry(entry.filename, &entry)) <<
        "pos: " << pos.ToString() << ", entry: " << entry.ToString();
  } else {
    // TODO(jonaso): seek offset in file using gtid-index
  }
  dst->Init(entry.filename, entry.start_position,
            entry.start_master_position);
  return true;
}

// Get local binlog position, file/pos which is currently being written to.
BinlogPosition Binlog::GetBinlogPosition() {
  absl::MutexLock lock(&position_mutex_);
  return position_;
}

bool Binlog::GetNextFile(FilePosition *pos) const {
  BinlogIndex::Entry entry;
  if (index_.GetNextEntry(pos->filename, &entry)) {
    pos->filename = entry.filename;
    return true;
  }
  return false;
}

/**
 * WaitBinlogEndPosition
 *
 * Wait for the binlog end position to move past pos.
 * @param pos - in/out -  in: highest position known by binlog reader
 *                       out: highest position known by binlog (writer)
 * @param timeout      - max duration to wait
 *
 * This function is used by a binlog reader. A reader will read binlog
 * up until an end position and once getting there, it will wait (using this
 * function). This construct allows the binlog writer to write things to
 * the binlog without a reader "seeing it".
 *
 */
bool Binlog::WaitBinlogEndPosition(FilePosition *pos,
                                   int64_t *truncate_counter,
                                   absl::Duration timeout) {
  absl::MutexLock lock(&position_mutex_);
  auto check = [this, pos]() {
    return stop_ || !position_.latest_completed_gtid_position.equal(*pos);
  };
  position_mutex_.AwaitWithTimeout(absl::Condition(&check), timeout);

  if (flushed_gtid_position_.equal(*pos) &&
      !position_.latest_completed_gtid_position.equal(*pos)) {
    absl::MutexLock file_lock(&file_mutex_);
    if (!binlog_file_->Flush()) {
      LOG(ERROR) << "Failed to flush binlog file";
      monitoring::rippled_binlog_error->Increment(monitoring::ERROR_FLUSH_FILE);
    } else {
      flushed_gtid_position_ = position_.latest_completed_gtid_position;
    }
  }

  bool retVal = !flushed_gtid_position_.equal(*pos);
  *pos = flushed_gtid_position_;
  *truncate_counter = truncate_counter_;
  return retVal;
}

void Binlog::Stop() {
  absl::MutexLock lock(&position_mutex_);
  stop_ = true;
}

// Connection established.
bool Binlog::ConnectionEstablished(const mysql::ClientConnection *con) {
  current_master_connection_ = con;
  return true;
}

bool Binlog::SkipWritingEvent(RawLogEventData event) const {
  switch (event.header.type) {
    case constants::ET_HEARTBEAT:
      return true;
    case constants::ET_GTID_LIST_MARIADB:
      return true;
    case constants::ET_BINLOG_CHECKPOINT:
      return true;
    case constants::ET_STOP:
      return true;
    case constants::ET_PREVIOUS_GTIDS_MYSQL:
      return true;
    default:
      return false;
  }
}

// Add an event to binlog.
// If wait is true, this method blocks until event has been written to disk.
bool Binlog::AddEvent(RawLogEventData event, bool wait) {
  if (event.header.type == constants::ET_HEARTBEAT) {
    // shortcut these here
    return true;
  }

  bool write_event = !SkipWritingEvent(event);
  // Note: FormatDescriptorEvents do not use Binlog::WriteEvent and thus we must
  // record the timestamp of these events (for monitoring purposes) here,
  // since the timestamp information is thrown away before the event is written
  // with WriteMasterFormatDescriptor.
  if (event.header.type == constants::ET_FORMAT_DESCRIPTION) {
    FormatDescriptorEvent ev;
    if (!ev.ParseFromRawLogEventData(event)) {
      LOG(ERROR) << "Failed to parse FormatDescriptorEvent!";
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_PARSE_FD);
      return false;
    }

    // we don't use mysql checksums in local binlog.
    ev.checksum = 0;

    absl::MutexLock position_lock(&position_mutex_);
    absl::MutexLock file_lock(&file_mutex_);
    if (ev.EqualExceptTimestamp(position_.master_format)) {
      monitoring::binlog_last_event_timestamp->Set(event.header.timestamp);
      monitoring::binlog_last_event_received->Set(
          absl::ToUnixSeconds(absl::Now()));
      return true;  // skip writing unneeded FDs
    }

    bool has_master_format = !position_.master_format.IsEmpty();
    position_.master_format = ev;
    position_.master_server_id.assign(event.header.server_id);

    bool success;
    if (has_master_format) {
      // This is a new format, switch file.
      // MasterFormatDescriptor is written inside switch file.
      success = SwitchFileLocked();
    } else {
      // We Didn't have any previous format.
      // Write the one that we got.
      CHECK(success = WriteMasterFormatDescriptor());
    }
    monitoring::binlog_last_event_timestamp->Set(event.header.timestamp);
    monitoring::binlog_last_event_received->Set(
        absl::ToUnixSeconds(absl::Now()));
    return success;
  }

  if (!ValidateEvent(event)) {
    LOG(ERROR) << "Failed to validate event!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_VALIDATE_EVENT);
    return false;
  }

  absl::MutexLock position_lock(&position_mutex_);
  off_t offset = position_.latest_event_end_position.offset;
  if (write_event) {
    absl::MutexLock file_lock(&file_mutex_);
    CHECK(WriteEvent(event, &offset, wait));
  } else {
    DLOG(INFO) << "Skip writing event " << event.ToString().c_str();
  }

  int res = position_.Update(event, offset);
  if (res == -1) {
    LOG(ERROR) << "Failed to update binlog position!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_UPDATE_BINLOG_POS);
    return false;
  } else if (res == 1) {
    DLOG(INFO) << "Update binlog position to end_pos: "
               << position_.latest_completed_gtid_position.ToString().c_str()
               << ", gtid: "
               << position_.latest_completed_gtid.ToString().c_str();
    if (wait) {
      flushed_gtid_position_ = position_.latest_completed_gtid_position;
    }
  }

  if (offset >= max_binlog_size_ && !position_.InTransaction()) {
    absl::MutexLock file_lock(&file_mutex_);
    return SwitchFileLocked();
  }

  return true;
}

bool Binlog::SwitchFile(std::string *newfile) {
  absl::MutexLock position_lock(&position_mutex_);
  absl::MutexLock file_lock(&file_mutex_);
  if (SwitchFileLocked()) {
    newfile->assign(position_.latest_event_end_position.filename);
    return true;
  }
  return false;
}

bool Binlog::SwitchFileLocked() {
  CloseFileLocked();
  BinlogPosition pos = position_;
  CHECK(index_.CloseEntry(pos.gtid_start_position, pos.next_master_position,
                          pos.latest_event_end_position.offset));
  bool ok = true;
  if (!Finalize(pos.latest_event_end_position.filename)) ok = false;
  // Immediately mark the file for archiving. The actual archiving will happen
  // automatically later, and the file will remain available at the same
  // path the whole time.
  if (!Archive(pos.latest_event_end_position.filename)) ok = false;
  CHECK(CreateNewFileLocked(pos.gtid_start_position, pos.next_master_position));
  if (!pos.master_format.IsEmpty())
      CHECK(WriteMasterFormatDescriptor());
  return ok;
}

// Connection closed.
void Binlog::ConnectionClosed(const mysql::ClientConnection *con) {
  assert(current_master_connection_ == con);
  current_master_connection_ = nullptr;
  BinlogPosition pos = GetBinlogPosition();
  LOG(INFO) << "Connection closed last position"
            << " binlog file: " << pos.latest_event_end_position.ToString()
            << ", gtid: " << pos.latest_start_gtid.ToString();
  bool truncated;
  if (!Rollback(&pos, &truncated)) {
    // There was transaction that was started but not completed.
    // When this happens we truncate() the binlog file to start clean.
    // If this Rollback fails, there is nothing to do...
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_ROLLBACK);
    LOG(FATAL) << "Rollback failed. Aborting";
  }

  absl::MutexLock position_lock(&position_mutex_);
  position_ = pos;
  if (truncated)
    truncate_counter_++;
}

// Validate event.
bool Binlog::ValidateEvent(RawLogEventData event) {
  BinlogPosition copy = GetBinlogPosition();
  off_t offset =
      copy.latest_event_end_position.offset + event.header.event_length;
  if (copy.Update(event, offset) == -1) {
    return false;
  }

  return true;
}

// Write an event to binlog file.
bool Binlog::WriteEvent(RawLogEventData event, off_t *offset, bool wait) {
  assert(event.header.event_length ==
         event.header.PackLength() + event.event_data_length);

  // Set correct nextpos
  event.header.nextpos =
      *offset + event.header.event_length + encryptor_->GetExtraSize();
  event.header.SerializeToBuffer(const_cast<uint8_t*>(event.event_buffer),
                                 event.header.PackLength());

  if (!encryptor_->Write(binlog_file_,
                         event.event_buffer, event.header.event_length)) {
    LOG(ERROR) << "Failed to write event";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_ENCRYPT);
    return false;
  }

  if (wait && !binlog_file_->Flush()) {
    LOG(ERROR) << "Failed to flush binlog file";
    monitoring::rippled_binlog_error->Increment(monitoring::ERROR_FLUSH_FILE);
    return false;
  }

  int64_t o;
  binlog_file_->Tell(&o);
  assert(o == event.header.nextpos);
  *offset = o;

  // TODO(jonaso): update gtid-index
  monitoring::binlog_last_event_timestamp->Set(event.header.timestamp);
  monitoring::binlog_last_event_received->Set(absl::ToUnixSeconds(absl::Now()));

  return true;
}

bool Binlog::GetBinlogSize(absl::string_view filename, off_t *size) const {
  int64_t sz;
  if (!ff_.Size(GetPath(filename), &sz)) {
    LOG(WARNING) << "Failed to stat file: " << filename;
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_STAT_FILE);
    return false;
  }
  *size = sz;
  return true;
}

std::string Binlog::GetPath(absl::string_view filename) const {
  return absl::StrCat(directory_, filename);
}

bool Binlog::Rollback(BinlogPosition *pos, bool *truncated) {
  absl::MutexLock file_lock(&file_mutex_);
  if (pos->latest_event_end_position.equal(
          pos->latest_completed_gtid_position)) {
    *truncated = false;
    return true;
  }
  const FilePosition& end = pos->latest_completed_gtid_position;
  LOG(INFO) << "Truncating binlog file: " << end.filename
            << ", to offset: " << end.offset
            << ", rollback to gtid: "
            << pos->latest_completed_gtid.ToString();
  bool binlog_open = binlog_file_ != nullptr;
  if (binlog_open) {
    // close/reopen binlog after truncation, so that
    // the file implementation doesn't get confused about offsets.
    binlog_file_->Close();
    binlog_file_ = nullptr;
  }
  file::AppendOnlyFile *file;
  if (!ff_.Open(&file, GetPath(end.filename), "r+")) {
    LOG(ERROR) << "Failed to open binlog file: " << end.filename;
    return false;
  }
  if (!file->Truncate(end.offset)) {
    file->Close();
    LOG(ERROR) << "Failed to truncate binlog file: " << end.filename
               << ", to offset: " << end.offset;
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_TRUNCATE_FILE);
    return false;
  }
  pos->latest_event_end_position = end;
  pos->latest_start_gtid = pos->latest_completed_gtid;
  pos->group_state = BinlogPosition::NO_GROUP;

  file->Close();
  if (binlog_open && !ff_.Open(&binlog_file_, GetPath(end.filename), "a")) {
    LOG(ERROR) << "Failed to reopen binlog file: " << end.filename;
    return false;
  }
  // signal that we truncated
  *truncated = true;
  return true;
}

void Binlog::RegisterReader(BinlogReader *reader) {
  absl::MutexLock mutex(&purge_mutex_);
  readers_.insert(reader);
}

void Binlog::UnregisterReader(BinlogReader *reader) {
  absl::MutexLock mutex(&purge_mutex_);
  auto it = readers_.find(reader);
  if (it != readers_.end()) {
    readers_.erase(it);
  }
}

bool Binlog::IsSafeToPurgeLocked(absl::string_view filename) const {
  for (BinlogReader *reader : readers_) {
    if (!reader->IsSafeToPurge(filename))
      return false;
  }
  return true;
}

bool Binlog::Remove(absl::string_view filename) {
  if (!ff_.Delete(GetPath(filename))) {
    LOG(ERROR) << "Failed to unlink " << GetPath(filename);
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_UNLINK_FILE);
    return false;
  }
  return true;
}

// Finalize this file, indicating it will not be written to any more.
bool Binlog::Finalize(absl::string_view filename) {
  if (!ff_.Finalize(GetPath(filename))) {
    LOG(ERROR) << "Failed to finalize " << GetPath(filename);
    monitoring::rippled_binlog_error->Increment(monitoring::ERROR_CHATTR_FILE);
    return false;
  }
  return true;
}

// Archive this file, possibly moving it to cheaper storage.
bool Binlog::Archive(absl::string_view filename) {
  if (!ff_.Archive(GetPath(filename))) {
    LOG(ERROR) << "Failed to archive " << GetPath(filename);
    monitoring::rippled_binlog_error->Increment(monitoring::ERROR_CHATTR_FILE);
    return false;
  }
  return true;
}

bool Binlog::PurgeLogs(std::string *oldest_file) {
  return PurgeLogsUntil("", oldest_file);
}

bool Binlog::PurgeLogsBefore(absl::Time before_time, std::string *oldest_file) {
  GTIDList pos;
  BinlogIndex::Entry entry;
  if (!index_.GetEntry(pos, &entry)) {
    LOG(WARNING) << "Failed to find first binlog index entry";
    return false;
  }

  int cnt = 0;
  while (entry.is_closed) {
    absl::Time mtime;
    if (!ff_.Mtime(GetPath(entry.filename), &mtime)) {
      LOG(WARNING) << "Failed to stat file: " << entry.filename;
      monitoring::rippled_binlog_error->Increment(monitoring::ERROR_STAT_FILE);
      return false;
    }

    if (mtime > before_time) {
      break;
    }

    cnt++;
    std::string filename = entry.filename;
    if (!index_.GetNextEntry(filename, &entry))
      break;
  }

  if (cnt > 0) {
    LOG(INFO) << "PurgeLogsBefore(" << before_time << ") => found " << cnt
              << " files";
  }
  return PurgeLogsUntil(entry.filename, oldest_file);
}

bool Binlog::PurgeLogsKeepSize(size_t keep_size, std::string *oldest_file) {
  GTIDList pos;
  BinlogIndex::Entry entry;
  if (!index_.GetEntry(pos, &entry)) {
    LOG(WARNING) << "Failed to find first binlog index entry";
    return false;
  }

  size_t total_size =
      index_.GetTotalSize() +
      GetBinlogPosition().latest_event_end_position.offset;

  int cnt = 0;
  while (entry.is_closed && (total_size - entry.file_size > keep_size)) {
    cnt++;
    total_size -= entry.file_size;

    std::string filename = entry.filename;
    if (!index_.GetNextEntry(filename, &entry))
      break;
  }

  if (cnt > 0) {
    LOG(INFO) << "PurgeLogsKeepSize(" << keep_size << ")"
              << " => found " << cnt << " files "
              << "(new total size: " << total_size << ")";
  }

  return PurgeLogsUntil(entry.filename, oldest_file);
}

bool Binlog::PurgeLogsUntil(absl::string_view to_file,
                            std::string *oldest_file) {
  GTIDList pos;
  BinlogIndex::Entry entry;
  if (!index_.GetEntry(pos, &entry)) {
    LOG(WARNING) << "Failed to find first binlog index entry";
    return false;
  }

  while (entry.is_closed && entry.filename != to_file) {
    BinlogIndex::Entry next_entry = entry;
    index_.GetNextEntry(entry.filename, &next_entry);
    {
      absl::MutexLock mutex(&purge_mutex_);
      if (!IsSafeToPurgeLocked(entry.filename)) {
        LOG(INFO) << "Unable to purge " << entry.filename
                  << ", file is in use";
        break;
      }
      if (!index_.MarkFirstEntryPurged(entry)) {
        LOG(ERROR) << "Failed to mark purged "
                   << entry.filename << " in index!";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_MARK_FILE_AS_PURGED);
        return false;
      } else {
        LOG(INFO) << "Marking " << entry.filename << " as purged";
      }
    }

    {
      absl::MutexLock lock(&position_mutex_);
      position_.gtid_purged = index_.GetOldestEntry().start_position;
    }

    if (!Remove(entry.filename)) {
      return false;
    }
    LOG(INFO) << "Purged " << entry.filename;

    if (!index_.PurgeFirstEntry(entry)) {
      LOG(ERROR) << "Failed to purge " << entry.filename << " from index!";
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_PURGE_FILE_FROM_INDEX);
      return false;
    }
    entry = next_entry;
  }
  oldest_file->assign(entry.filename);
  return true;
}

}  // namespace mysql_ripple
