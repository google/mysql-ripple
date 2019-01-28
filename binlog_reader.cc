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

#include "binlog_reader.h"

#include "logging.h"
#include "monitoring.h"
#include "mysql_constants.h"

namespace mysql_ripple {

BinlogReader::BinlogReader(const file::Factory &ff,
                           BinlogReader::BinlogInterface *binlog,
                           BinlogEndPositionProviderInterface *binlog_endpos)
    : binlog_(binlog),
      binlog_endpos_(binlog_endpos != nullptr ? binlog_endpos : binlog),
      encryptor_(BinlogEncryptorFactory::GetInstance(0)),
      ff_(ff),
      binlog_file_(nullptr),
      truncate_counter_(0),
      seek_completed_(false) {}

BinlogReader::~BinlogReader() { CloseFile(); }

bool BinlogReader::Open(GTIDList *pos, std::string *message) {
  position_.Reset();
  // Note: we must NOT hold mutex_ when calling (Un)RegisterReader or
  // we might deadlock due to locking mutexes in opposite order.
  binlog_->RegisterReader(this);
  // 1) Find correct file and approximate offset.
  // This is stored in binlog index which one accesses via the binlog class.
  if (binlog_->GetPosition(*pos, &position_, message)) {
    // Set end_of_file_ to latest_event_end_position, this
    // will cause ReadEvent() to "refresh", i.e call WaitBinlogEndPosition.
    end_of_file_ = position_.latest_event_end_position.offset;

    // Seek to exact position.
    if (Seek(pos, message))
      return true;
  }

  LOG(ERROR) << *message;

  // Note: we must NOT hold mutex_ when calling (Un)RegisterReader or
  // we might deadlock due to locking mutexes in opposite order.
  binlog_->UnregisterReader(this);
  return false;
}

bool BinlogReader::Open(const BinlogIndex::Entry& file) {
  // this function is only used during binlog recovery,
  // so we don't need to register at binlog (to prevent purging).
  SetCurrentFile(file.filename);
  position_.Init(file.filename, file.start_position,
                 file.start_master_position);
  end_of_file_ = position_.latest_event_end_position.offset;
  return true;
}

// Close an opened binlog.
bool BinlogReader::Close() {
  CloseFile();
  // Note: we must NOT hold mutex_ when calling (Un)RegisterReader or
  // we might deadlock due to locking mutexes in opposite order.
  binlog_->UnregisterReader(this);
  return true;
}

file_util::ReadResultCode BinlogReader::ReadEvent(RawLogEventData *event,
                                                  absl::Duration timeout) {
  if (position_.latest_event_end_position.offset == end_of_file_) {
    // We have read all the way up to latest end of current binlog.
    // Wait for that to change.
    int64_t truncate_counter;
    FilePosition end_pos = position_.latest_event_end_position;
    if (!binlog_endpos_->WaitBinlogEndPosition(&end_pos, &truncate_counter,
                                               timeout)) {
      event->header.event_length = 0;
      return file_util::READ_OK;
    }

    if (truncate_counter != truncate_counter_) {
      // Binlog has been truncated, we need to reopen file
      // so that FILE* has not buffered something that was
      // removed.
      truncate_counter_ = truncate_counter;
      ReopenBinlogFile();
    }

    // When WaitBinlogEndPosition returns true => the position has changed.
    // Check if binlog has moved on to new file.
    if (!end_pos.filename.compare(
            position_.latest_event_end_position.filename)) {
      // It hasn't. In that case current end of file is returned in end_pos
      end_of_file_ = end_pos.offset;
    } else {
      // It has, get size of current file.
      CHECK(binlog_->GetBinlogSize(position_.latest_event_end_position.filename,
                                   &end_of_file_));
      if (position_.latest_event_end_position.offset == end_of_file_) {
        // binlog has moved to next file
        // and we have read everything in current file.
        if (!SwitchFile()) {
          return file_util::READ_ERROR;
        }
        return ReadEvent(event, timeout);
      }
    }
  }

  switch (Read(&buffer_)) {
    case file_util::READ_OK:
      break;
    case file_util::READ_ERROR:
      LOG(ERROR) << "Failure while reading log event"
                 << ", " << position_.latest_event_end_position.ToString();
      monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_READ_EVENT);
      return file_util::READ_ERROR;
    case file_util::READ_EOF:
      LOG(WARNING) << "Got EOF while reading log event"
                   << ", " << position_.latest_event_end_position.ToString()
                   << " eof: " << end_of_file_;
      return file_util::READ_EOF;
  }

  if (!event->ParseFromBuffer(buffer_.data(), buffer_.size())) {
    LOG(ERROR) << "Failure while parsing log event"
               << ", " << position_.latest_event_end_position.ToString();
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_PARSE_EVENT);
    return file_util::READ_ERROR;
  }

  if (event->header.type == constants::ET_START_ENCRYPTION) {
    BinlogEncryptor *encryptor = BinlogEncryptorFactory::GetInstance(*event);
    if (encryptor == nullptr) {
      LOG(ERROR) << "Failure while creating encryptor"
                 << ", " << position_.latest_event_end_position.ToString();
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_INIT_ENCRYPTOR);
      return file_util::READ_ERROR;
    }
    encryptor_.reset(encryptor);
  }

  int64_t offset;
  binlog_file_->Tell(&offset);
  absl::MutexLock lock(&mutex_);
  if (position_.Update(*event, offset) == -1) {
    LOG(ERROR) << "Failed to update binlog position"
               << ", offset: " << offset;
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_UPDATE_BINLOG_POS);

    return file_util::READ_ERROR;
  }

  return file_util::READ_OK;
}

bool BinlogReader::Seek(GTIDList *pos, std::string *msg) {
  if (pos->IsEmpty()) {
    absl::MutexLock lock(&mutex_);
    seek_completed_ = true;
    return true;
  }

  while (!GTIDList::Subset(*pos, position_.gtid_start_position)) {
    RawLogEventData event;
    switch (ReadEvent(&event, absl::ZeroDuration())) {
      case file_util::READ_ERROR:
        *msg =
            "Fatal error while reading binlog (@" +
            position_.latest_event_start_position.ToString() + ")";
        return false;
      case file_util::READ_OK:
        if (event.header.event_length !=0)
          break;
        ABSL_FALLTHROUGH_INTENDED;
      case file_util::READ_EOF:
        *msg =
            "Fatal error. Requested to start reading binlog from position: "
            + pos->ToString() + ", but max found: "
            + position_.gtid_start_position.ToString();
        return false;
    }
  }

  if (!position_.gtid_start_position.Equal(* pos)) {
    *msg = "Error: connecting slave requested to start from GTID " +
        pos->ToString() +
        ", which is not in the master's binlog. Since the "
        "master's binlog contains GTIDs with higher sequence numbers, "
        "it probably means that the slave has diverged due to "
        "executing extra erroneous transactions, found (" +
        position_.gtid_start_position.ToString() + ")";
    monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_READER_SEEK);
    return false;
  }

  DLOG(INFO) << "Seek completed"
             << ", position: " << position_.ToString();

  absl::MutexLock lock(&mutex_);
  seek_completed_ = true;
  return true;
}

bool BinlogReader::OpenFile() {
  if (binlog_file_) return true;
  file::InputFile *file;
  auto filename = position_.latest_event_end_position.filename;
  auto res = OpenAndValidate(&file, filename);
  switch (res) {
    case file_util::OK:
      break;
    case file_util::NO_SUCH_FILE:
      LOG(ERROR) << "No such file opening binlog "
                 << position_.latest_event_end_position.filename;
      monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_BINLOG_FILE_NOT_FOUND);
      return false;
    case file_util::FILE_EMPTY:
      LOG(ERROR) << "Empty file opening binlog "
                 << position_.latest_event_end_position.filename;
      monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INVALID_MAGIC);
      return false;
    case file_util::INVALID_MAGIC:
      LOG(ERROR) << "Invalid magic opening binlog "
                 << position_.latest_event_end_position.filename;
      monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INVALID_MAGIC);
      return false;
    default:
      LOG(ERROR) << "Unexpected result code " << res << " opening binlog "
                 << position_.latest_event_end_position.filename;
      return false;
  }

  binlog_file_ = file;
  int64_t pos;
  binlog_file_->Tell(&pos);
  position_.OpenFile(filename, pos);
  return true;
}

void BinlogReader::CloseFile() {
  if (binlog_file_ != nullptr) {
    binlog_file_->Close();
    binlog_file_ = nullptr;
    // assume file is unencrypted until StartEncryptionEvent is read
    encryptor_.reset(BinlogEncryptorFactory::GetInstance(0));
  }
}

void BinlogReader::ReopenBinlogFile() {
  if (binlog_file_ == nullptr)
    return;

  file::InputFile *file;
  auto filename = position_.latest_event_end_position.filename;
  auto res = OpenAndValidate(&file, filename);
  CHECK_EQ(res, file_util::OK)
      << "Failed to reopen binlog file: "
      << position_.latest_event_end_position.filename << ", code: " << res;
  CHECK(file->Seek(position_.latest_event_end_position.offset))
      << "Failed to seek in reopened file";
  binlog_file_->Close();
  binlog_file_ = file;
}


bool BinlogReader::SwitchFile() {
  FilePosition pos = position_.latest_event_end_position;
  CHECK(pos.offset == end_of_file_);  // Only switchfile if we're at the end
  DLOG(INFO) << "switchfile from: " << pos.ToString();
  if (binlog_->GetNextFile(&pos)) {
    CloseFile();
    SetCurrentFile(pos.filename);
    return true;
  }
  LOG(ERROR) << "Failed to switch file from " << pos.filename
             << " while reading binlog";
  monitoring::rippled_binlog_error->Increment(
    monitoring::ERROR_READER_SWITCH_FILE);
  return false;
}

void BinlogReader::SetCurrentFile(absl::string_view filename) {
  position_.own_format.Reset();
  position_.master_format.Reset();
  position_.latest_event_end_position.filename = std::string(filename);
  position_.latest_event_end_position.offset = 0;
  end_of_file_ = 0;
}

file_util::ReadResultCode BinlogReader::Read(Buffer *dst) {
  if (!OpenFile()) {
    LOG(ERROR) << "Open failed when reading binlog";
    monitoring::rippled_binlog_error->Increment(monitoring::ERROR_OPEN_FILE);
    return file_util::READ_ERROR;
  }

  auto read_result = encryptor_->Read(binlog_file_, end_of_file_, dst);
  if (read_result == file_util::READ_ERROR) {
    LOG(ERROR) << "Error while reading binlog";
    monitoring::rippled_binlog_error->Increment(monitoring::ERROR_READ_FILE);
    monitoring::rippled_binlog_error->Increment(monitoring::ERROR_DECRYPT);
  }
  return read_result;
}

file_util::OpenResultCode BinlogReader::Validate(absl::string_view filename) {
  file::InputFile *file;
  auto res = OpenAndValidate(&file, filename);
  if (res == file_util::OK) {
    file->Close();
  }
  return res;
}

// Check if it's safe to purge a file.
bool BinlogReader::IsSafeToPurge(absl::string_view filename) const {
  absl::MutexLock lock(&mutex_);
  auto last_filename = position_.latest_event_end_position.filename;
  if (last_filename.empty()) {
    // We have called RegisterReader, but not yet GetPosition()
    // block any purge.
    return false;
  }
  return last_filename != filename;
}

file_util::OpenResultCode BinlogReader::OpenAndValidate(
    file::InputFile **file, absl::string_view filename) {
  std::string fn(filename);
  auto path = binlog_->GetPath(fn);
  absl::string_view header(constants::BINLOG_HEADER,
                           sizeof(constants::BINLOG_HEADER));
  return file_util::OpenAndValidate(file, ff_, path, "r", header);
}

}  // namespace mysql_ripple
