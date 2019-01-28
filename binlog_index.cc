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

#include "binlog_index.h"

#include <unistd.h>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "file.h"
#include "logging.h"
#include "monitoring.h"

namespace mysql_ripple {

constexpr const char HEADER[] = "# this is a binlog index for ripple\n";

BinlogIndex::BinlogIndex(const char* directory, const file::Factory& ff)
    : directory_(directory),
      basename_("binlog"),
      ff_(ff),
      index_file_(nullptr) {
  if (directory_.back() != '/')
    directory_ += "/";
}

BinlogIndex::~BinlogIndex() {
  Close();
}

void BinlogIndex::SetBasename(absl::string_view basename) {
  basename_ = std::string(basename);
}

const std::string& BinlogIndex::GetBasename() const {
  return basename_;
}

std::string BinlogIndex::GetIndexFilename() const {
  return directory_ + basename_ + ".index";
}

// Create binlog index and open if non-exists.
// If a binlog index already exists return false.
bool BinlogIndex::Create() {
  std::string name = GetIndexFilename();
  file::AppendOnlyFile* f;
  if (!ff_.Create(&f, name, "w")) {
    LOG(ERROR) << "Failed to create binlog index file";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_CREATE_FILE);
    return false;
  }
  if (!f->Write(HEADER)) {
    LOG(ERROR) << "Failed to write binlog index file header";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_FILE);
    f->Close();
    return false;
  }
  f->Sync();

  {
    absl::MutexLock lock(&file_mutex_);
    CHECK(index_file_ == nullptr);
    index_file_ = f;
    last_file_line_is_open_ = false;
  }

  {
    absl::MutexLock lock(&entries_mutex_);
    index_entries_.clear();
  }
  return true;
}

// Open binlog index and recover/discard unfinished entries.
int BinlogIndex::Recover(BinlogRecoveryHandlerInterface *handler) {
  index_entries_.clear();
  file::InputFile* f;

  auto res =
      file_util::OpenAndValidate(&f, ff_, GetIndexFilename(), "r", HEADER);
  switch (res) {
    case file_util::NO_SUCH_FILE:
    case file_util::FILE_EMPTY:
      return 0;
    case file_util::INVALID_MAGIC:
      LOG(ERROR) << "Failed to open binlog index - invalid magic";
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_INVALID_MAGIC);
      return -1;
    case file_util::OK:
      break;
  }

  // this struct is used only during recover.
  struct recover_entry {
    int lineno;
    file_util::OpenResultCode open_file_result;
  };
  std::vector<recover_entry> recover_entries;

  // read all lines in file (and then validate consistency).
  int lineno = 1;
  Buffer buf;
  bool done = false;
  while (!done || !buf.empty()) {
    auto pos = std::find(std::begin(buf), std::end(buf), '\n');
    while (!done && pos == std::end(buf)) {
      auto len = buf.size();
      // read more data until we find a newline.
      done = !f->Read(buf, 256);
      pos = std::find(std::begin(buf) + len, std::end(buf), '\n');
    }
    if (pos != std::end(buf)) pos++;
    std::string line(std::begin(buf), pos);
    buf.erase(std::begin(buf), pos);

    Entry entry;
    recover_entry recover_info;
    recover_info.lineno = lineno;
    lineno++;
    if (line.empty()) {
      // empty line...should be EOF
      if (!f->eof()) {
        f->Close();
        LOG(ERROR) << "Inconsistent binlog-index"
                   << ", line: " << lineno
                   << ", found unexpected empty line";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCONSISTENT_INDEX);
        return -1;
      }

      break;  // end of file
    } else {
      if (line[0] == '#')
        continue;

      if (!entry.Parse(line)) {
        f->Close();
        LOG(ERROR) << "Inconsistent binlog-index"
                   << ", line: " << lineno
                   << ", failed to parse line: \"" << line << "\"";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCONSISTENT_INDEX);
        return -1;
      }

      recover_info.open_file_result = handler->Validate(entry.filename);
      recover_entries.push_back(recover_info);
      entry.file_size = handler->GetFileSize(entry.filename);
      index_entries_.push_back(entry);
    }
  }

  if (!f->eof()) {
    f->Close();
    LOG(ERROR) << "Error reading binlog-index. "
               << monitoring::ERROR_FAILED_READ_INDEX;
    monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_FAILED_READ_INDEX);
    return -1;
  }

  CHECK(index_entries_.size() == recover_entries.size());

  bool ok = true;

  /**
   * If an entry has a last_position, subsequent entries must have
   * start_position.
   *
   * GOOD:
   * 0: start_position='' last_position='1-2-3'
   * 1: start_position='1-2-3' last_position='1-2-4'
   *
   * BAD:
   * 0: start_position='' last_position='1-2-3'
   * 1: start_position='' last_position=''
   */
  // entry[N-1].last_position != nullptr => entry[N].start_position != nullptr
  for (size_t n = 1; n < index_entries_.size(); n++) {
    if (!index_entries_[n-1].last_position.IsEmpty()) {
      if (index_entries_[n].start_position.IsEmpty()) {
        LOG(ERROR) << "Inconsistent binlog-index"
                   << ", entry: " << n
                   << ", missing or empty start position";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCONSISTENT_INDEX);
        ok = false;
      }
    }
  }

  /**
   * For all entries except the last in the file,
   *   If entry has a start_position it must have last_position.
   *
   * GOOD:
   * 0: start_position='' last_position=''
   * 1: start_position='' last_position=''
   *
   * GOOD:
   * 0: start_position='1-2-3' last_position='1-2-3'
   * 1: start_position='1-2-3'
   *
   * BAD:
   * 0: start_position='1-2-3' last_position=''
   * 1: start_position='1-2-3'
   */
  // N < last: entry[N].start_position != nullptr =>
  // entry[N].last_position != nullptr
  for (size_t n = 0; n + 1 < index_entries_.size(); n++) {
    if (!index_entries_[n].start_position.IsEmpty()) {
      if (index_entries_[n].last_position.IsEmpty()) {
        LOG(ERROR) << "Inconsistent binlog-index"
                   << ", entry: " << n
                   << ", missing or empty last position";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCONSISTENT_INDEX);
        ok = false;
      }
    }
  }

  // entry[N].last_pos != nullptr => entry[N].last_next_master_pos != nullptr
  for (size_t n = 0; n < index_entries_.size(); n++) {
    if (!index_entries_[n].last_position.IsEmpty()) {
      if (index_entries_[n].last_next_master_position.IsEmpty()) {
        LOG(ERROR) << "Inconsistent binlog-index"
                   << ", entry: " << n
                   << ", missing or empty last next master position";
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INCONSISTENT_INDEX);
        ok = false;
      }
    }
  }

  /**
   * Each entry except the last in the file must have a Valid binlog file.
   * The index-entry is created before the actual file is created (this is
   * done to avoid dangling binlog files), but if process crashes after
   * index entry is created, but before binlog file is created a index-entry
   * without binlog file will be found on recovery. This shall however
   * only be possible for last entry in binlog index.
   *
   * Note: also purged-marked files might be missing
   *
   * see Binlog::CreateNewFile()
   */
  // Only last entry may have incomplete binlog file
  for (size_t n = 0; n + 1 < recover_entries.size(); n++) {
    if (!(recover_entries[n].open_file_result == file_util::OK ||
          (index_entries_[n].is_purged == true &&
           recover_entries[n].open_file_result == file_util::NO_SUCH_FILE))) {
      LOG(ERROR) << "Inconsistent binlog-index"
                 << ", entry: " << n
                 << ", binlog file not valid: "
                 << index_entries_[n].filename;
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_INCONSISTENT_INDEX);
      ok = false;
    }
  }

  // only last entry can be !is_closed
  for (size_t n = 0; n + 1 < index_entries_.size(); n++) {
    if (!index_entries_[n].is_closed) {
      LOG(ERROR) << "Inconsistent binlog-index"
                 << ", entry: " << n
                 << ", only last entry can be open";
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_INCONSISTENT_INDEX);
      ok = false;
    }
  }

  // Check state of last entry
  if (!index_entries_.empty()) {
    // last entry can't be purged
    if (index_entries_.back().is_purged) {
      LOG(ERROR) << "Inconsistent binlog-index"
                 << ", entry: " << (index_entries_.size() - 1)
                 << ", last entry can't be purged";
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_INCONSISTENT_INDEX);
      ok = false;
    }

    switch (recover_entries.back().open_file_result) {
      case file_util::NO_SUCH_FILE:
      case file_util::FILE_EMPTY:
        if (!index_entries_.back().last_position.IsEmpty()) {
          // If last_position isn't empty
          // then file for last entry should be OK
          LOG(ERROR) << "Inconsistent binlog-index"
                     << ", entry: " << (index_entries_.size() - 1)
                     << ", binlog file not valid: "
                     << index_entries_.back().filename;
          monitoring::rippled_binlog_error->Increment(
            monitoring::ERROR_INCONSISTENT_INDEX);
          ok = false;
        }
        break;
      case file_util::OK:
        break;
      case file_util::INVALID_MAGIC:
        LOG(ERROR) << "Inconsistent binlog-index"
                   << ", entry: " << (index_entries_.size() - 1)
                   << ", binlog file not valid: "
                     << index_entries_.back().filename;
        monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_INVALID_MAGIC);

        ok = false;
        break;
    }
  }

  f->Close();
  if (!ok) {
    return -1;
  }

  // We think that state is OK, now check what to do...
  // TODO(nerdatmath): shouldn't we hold the lock here?
  if (index_file_ != nullptr) {
    index_file_->Close();
  }
  if (!ff_.Open(&index_file_, GetIndexFilename(), "a")) return -1;
  last_file_line_is_open_ =
      !index_entries_.empty() && index_entries_.back().last_position.IsEmpty();

  if (index_entries_.empty()) {
    // This is equivalent to an empty file
    return 1;
  }

  // purge entries that was marked as purged.
  bool purged = false;
  for (size_t n = 0; n + 1 < index_entries_.size(); n++) {
    if (index_entries_[n].is_purged &&
        recover_entries[n].open_file_result != file_util::NO_SUCH_FILE) {
      purged = true;
      if (!handler->Remove(index_entries_[n].filename)) {
        return false;
      }
    }
  }

  if (purged) {
    std::vector<Entry> copy;
    for (size_t n = 0; n < index_entries_.size(); n++) {
      if (!index_entries_[n].is_purged) {
        copy.push_back(index_entries_[n]);
      }
    }
    index_entries_.swap(copy);
  }

  bool truncated = false;
  if (recover_entries.back().open_file_result != file_util::OK) {
    truncated = true;
    // last file wasn't OK, it was empty/non-existent.
    // truncate last entry in index-file.
    LOG(INFO) << "Last entry of binlog index was empty"
              << ", removing that from index file"
              << ", empty file: " << index_entries_.back().filename;
    index_entries_.pop_back();
  }

  if (purged || truncated) {
    if (!RewriteIndex(index_entries_)) {
      LOG(ERROR) << "Failed to rewrite index";
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_FAILED_REWRITE_INDEX);
      return -1;
    }
  }

  return 1;
}

// Check if binlog index is open.
bool BinlogIndex::IsOpen() {
  absl::MutexLock lock(&file_mutex_);
  return index_file_ != nullptr;
}

// Close an opened binlog index.
bool BinlogIndex::Close() {
  absl::MutexLock lock(&file_mutex_);
  if (index_file_ != nullptr) {
    index_file_->Sync();
    index_file_->Close();
    index_file_ = nullptr;
  }
  return true;
}

BinlogIndex::Entry BinlogIndex::GetOldestEntry() const {
  absl::MutexLock lock(&entries_mutex_);
  for (const Entry& entry : index_entries_) {
    if (entry.is_purged)
      continue;
    return entry;
  }

  Entry entry;
  entry.Reset();
  return entry;
}

BinlogIndex::Entry BinlogIndex::GetCurrentEntry() const {
  absl::MutexLock lock(&entries_mutex_);
  if (index_entries_.empty()) {
    Entry entry;
    entry.Reset();
    return entry;
  }
  return index_entries_.back();
}

// Add a new entry to the index.
bool BinlogIndex::NewEntry(const GTIDList& start_position,
                           const FilePosition& master_position) {
  // Check that last file is closed first.
  Entry entry = GetCurrentEntry();
  if (!entry.IsEmpty()) {
    if (entry.last_position.IsEmpty() && !entry.start_position.IsEmpty()) {
      LOG(ERROR) << "Creating new file when last is not closed"
                 << ", old_file: " << entry.ToString().c_str();
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_OLD_FILE_NOT_CLOSED);
      return false;
    }
  }

  entry.is_closed = false;
  entry.is_purged = false;
  entry.start_position = start_position;
  entry.start_master_position = master_position;
  entry.last_position.Reset();
  entry.last_next_master_position.Reset();
  entry.filename = GetNextFilename(entry.filename);
  std::string str = entry.FormatHead();

  {
    absl::MutexLock lock(&file_mutex_);
    CHECK(!last_file_line_is_open_);
    if (!(index_file_->Write(str) && index_file_->Sync())) {
      index_file_->Close();
      index_file_ = nullptr;
      LOG(ERROR) << "Failed to write entry to binlog index"
                 << ", size: " << str.size();
      monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_WRITE_INDEX_ENTRY);
      return false;
    }
    last_file_line_is_open_ = true;
  }

  {
    absl::MutexLock lock(&entries_mutex_);
    index_entries_.push_back(entry);
  }
  return true;
}

// End current entry.
bool BinlogIndex::CloseEntry(const GTIDList& last_position,
                             const FilePosition& last_next_master_position,
                             size_t file_size) {
  Entry entry = GetCurrentEntry();

  if (!entry.start_position.IsEmpty()) {
    if (last_position.IsEmpty())
      return false;
  }

  if (!entry.start_master_position.IsEmpty()) {
    if (last_next_master_position.IsEmpty())
      return false;
  }

  entry.is_closed = true;
  entry.last_position = last_position;
  entry.last_next_master_position = last_next_master_position;
  entry.file_size = file_size;
  std::string str = entry.FormatTail();

  {
    absl::MutexLock lock(&file_mutex_);
    CHECK(last_file_line_is_open_);
    if (!(index_file_->Write(str) && index_file_->Sync())) {
      index_file_->Close();
      index_file_ = nullptr;
      LOG(ERROR) << "Failed to write entry-tail to binlog index"
                 << ", size: " << str.size();
      monitoring::rippled_binlog_error->Increment(
          monitoring::ERROR_WRITE_INDEX_ENTRY);
      return false;
    }
    last_file_line_is_open_ = false;
  }

  {
    absl::MutexLock lock(&entries_mutex_);
    index_entries_.back() = entry;
  }
  return true;
}

std::string BinlogIndex::Entry::FormatHead() const {
  std::string tmp;
  std::string start_pos;
  start_position.SerializeToString(&start_pos);
  tmp += "filename=" + filename;
  tmp += " start_pos='" + start_pos + "'";
  if (!start_master_position.IsEmpty()) {
    tmp += " start_master_pos="+start_master_position.ToString();
  }
  return tmp;
}

std::string BinlogIndex::Entry::FormatTail() const {
  if (!start_position.IsEmpty()) {
    assert(!last_position.IsEmpty());
  }
  std::string tmp;
  std::string end_pos;

  last_position.SerializeToString(&end_pos);
  tmp += " end_pos='" + end_pos + "'";

  if (!last_next_master_position.IsEmpty()) {
    tmp += " last_next_master_pos=" + last_next_master_position.ToString();
  }

  if (is_purged) {
    tmp += " purged=1";
  }

  return tmp + "\n";
}

bool BinlogIndex::Entry::Parse(absl::string_view line) {
  Reset();
  is_closed = line.back() == '\n';
  if (is_closed) line.remove_suffix(1);
  std::vector<absl::string_view> v = absl::StrSplit(line, ' ');

  for (auto s : v) {
    const auto filename_len = sizeof("filename=") - 1;
    const auto start_pos_len = sizeof("start_pos=") - 1;
    const auto end_pos_len = sizeof("end_pos=") - 1;
    const auto start_master_pos_len = sizeof("start_master_pos=") - 1;
    const auto last_next_master_pos_len = sizeof("last_next_master_pos=") - 1;
    const auto purged_len = sizeof("purged=") - 1;
    if (s.compare(0, filename_len, "filename=") == 0) {
      filename = std::string(s.substr(filename_len));
    } else if (s.compare(0, start_pos_len, "start_pos=") == 0) {
      if (!start_position.Parse(s.substr(start_pos_len))) {
        return false;
      }
    } else if (s.compare(0, end_pos_len, "end_pos=") == 0) {
      if (!last_position.Parse(s.substr(end_pos_len))) {
        return false;
      }
    } else if (s.compare(0, start_master_pos_len, "start_master_pos=") == 0) {
      if (!start_master_position.Parse(s.substr(start_master_pos_len))) {
        return false;
      }
    } else if (s.compare(0, last_next_master_pos_len,
                         "last_next_master_pos=") == 0) {
      if (!last_next_master_position.Parse(
              s.substr(last_next_master_pos_len))) {
        return false;
      }
    } else if (s.compare(0, purged_len,
                         "purged=") == 0) {
      if (!absl::SimpleAtob(s.substr(purged_len), &is_purged)) {
        return false;
      }
    } else {
      // allow other strings on this line...
    }
  }
  return true;
}

std::string BinlogIndex::GetNextFilename(absl::string_view filename) const {
  uint32_t n = 0;
  size_t found = filename.find_last_of(".");
  if (found != absl::string_view::npos) {
    if (absl::SimpleAtoi(filename.substr(found + 1), &n)) {
      n++;
    } else {
      n = 0;
    }
  }
  return absl::StrCat(basename_, ".", absl::Dec(n, absl::kZeroPad6));
}

std::string BinlogIndex::Entry::ToString() const {
  std::string tmp;
  tmp += "[ ";
  tmp += filename;
  if (!start_position.IsEmpty())
    tmp += " start=" + start_position.ToString();
  if (!start_master_position.IsEmpty())
    tmp += " start_master_position=" + start_master_position.ToString();
  if (!last_position.IsEmpty())
    tmp += " end_pos=" + last_position.ToString();
  if (!last_next_master_position.IsEmpty())
    tmp += " last_next_master_pos=" + last_next_master_position.ToString();
  tmp += " ]";
  return tmp;
}

bool BinlogIndex::GetEntry(const GTIDList& pos, Entry *dst,
                           std::string *message) const {
  absl::MutexLock lock(&entries_mutex_);
  if (index_entries_.empty()) return false;

  for (const Entry& entry : index_entries_) {
    if (entry.is_purged)
      continue;

    if (pos.IsEmpty()) {
      // start from the beginning...
      *dst = entry;
      return true;
    }

    // we scanned too far...
    if (!GTIDList::Subset(entry.start_position, pos)) {
      if (message != nullptr) {
        *message =
            "Fatal error. "
            "Connecting slave requested to start from GTID " + pos.ToString() +
            ", which is not in the master's binlog "
            "(min found " + entry.start_position.ToString() + ")";
      }
      return false;
    }

    if (GTIDList::Subset(pos, entry.last_position)) {
      *dst = entry;
      return true;
    }

    // GTIDList was not found in older entries and
    // this entry is still open, search here.
    if (!entry.is_closed) {
      *dst = entry;
      return true;
    }
  }

  if (message != nullptr) {
    *message =
        "Fatal error. "
        "Connecting slave requested to start from GTID " + pos.ToString() +
        ", which is not in the master's binlog.";
  }

  return false;
}

bool BinlogIndex::GetNextEntry(absl::string_view filename, Entry* dst) const {
  absl::MutexLock lock(&entries_mutex_);
  for (size_t n = 0; n + 1 < index_entries_.size(); n++) {
    if (index_entries_[n].is_purged)
      continue;
    if (index_entries_[n].filename == filename) {
      *dst = index_entries_[n+1];
      return true;
    }
  }
  return false;
}

bool BinlogIndex::RewriteIndex(const std::vector<Entry>& entries) {
  absl::MutexLock lock(&file_mutex_);
  std::string real_name = GetIndexFilename();
  std::string tmp_name = real_name + ".tmp";

  // remove any old version
  ff_.Delete(tmp_name);

  file::AppendOnlyFile* f;
  if (!ff_.Create(&f, tmp_name, "w")) {
    LOG(ERROR) << "Failed to create temp binlog index file";
    return false;
  }
  if (!f->Write(HEADER)) {
    LOG(ERROR) << "Failed to write binlog index header";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_FILE);
    f->Close();
    return false;
  }
  bool last_file_line_is_open = false;

  for (const Entry& entry : entries) {
    std::string str = entry.FormatHead();
    CHECK(!last_file_line_is_open);
    if (entry.is_closed) {
      str += entry.FormatTail();
    }
    if (!f->Write(str)) {
      LOG(ERROR) << "Failed to write entry to binlog index (tmp)"
                 << ", size: " << str.size();
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_WRITE_INDEX_ENTRY);
      monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_WRITE_FILE);
      f->Close();
      return false;
    }
    last_file_line_is_open = str.back() != '\n';
  }

  if (!f->Sync() || !f->Close()) {
    LOG(ERROR) << "Failed to sync and close tmp binlog index";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_WRITE_FILE);
    return false;
  }

  if (!ff_.Rename(tmp_name, real_name)) {
    LOG(ERROR) << "Failed to rename tmp binlog index";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_RENAME_FILE);
    return false;
  }

  if (index_file_ != nullptr) {
    index_file_->Close();
    index_file_ = nullptr;
  }
  if (!ff_.Open(&index_file_, real_name, "a")) return false;
  last_file_line_is_open_ = last_file_line_is_open;
  return true;
}

bool BinlogIndex::MarkFirstEntryPurged(const Entry& entry) {
  // Copy entries, so we can modify the found entry.
  std::vector<Entry> copy;
  {
    absl::MutexLock lock(&entries_mutex_);
    copy = index_entries_;
  }

  // Locate entry
  if (copy.size() <= 1) {
    LOG(ERROR) << "Failed to mark entry " << entry.filename
               << " as purged, only " << copy.size()
               << " entry in index!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_MARK_ENTRY_AS_PURGED);
    return false;  // not found
  }

  // One can only purge oldest entry.
  if (copy.front().filename.compare(entry.filename) != 0) {
    LOG(ERROR) << "Failed to mark entry " << entry.filename
               << " as purged, oldest entry is: "
               << copy.front().filename;
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_MARK_ENTRY_AS_PURGED);
    return false;  // not found
  }

  // Entry already marked as purged...
  if (copy.front().is_purged) {
    LOG(ERROR) << "Failed to mark entry " << entry.filename
               << " as purged, it is already marked!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_MARK_ENTRY_AS_PURGED);
    return false;
  }


  // Modify copy
  copy.front().is_purged = true;

  // Write this to disk
  if (!RewriteIndex(copy)) {
    LOG(ERROR) << "Failed to purge mark entry " << entry.filename
               << ", rewrite of index file failed!";
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_MARK_ENTRY_AS_PURGED);
    return false;
  }

  // And finally update in memory version
  {
    absl::MutexLock lock(&entries_mutex_);
    index_entries_.front().is_purged = true;
  }

  return true;
}

bool BinlogIndex::PurgeFirstEntry(const Entry& entry) {
  std::vector<Entry> copy;
  {
    absl::MutexLock lock(&entries_mutex_);
    copy = index_entries_;
  }

  // Locate entry
  if (copy.size() <= 1)
    return false;  // not found

  // One can only purge oldest entry.
  if (copy.front().filename.compare(entry.filename) != 0)
    return false;  // not found

  // It needs to be marked first.
  if (copy.front().is_purged != true) {
    LOG(ERROR) << "Failed to purge entry, entry.is_purged != true";
    return false;
  }

  // Remove the in memory version
  {
    absl::MutexLock lock(&entries_mutex_);
    index_entries_.erase(index_entries_.begin(), index_entries_.begin()+1);
    copy = index_entries_;
  }

  // And rewrite index again
  return RewriteIndex(copy);
}

bool BinlogIndex::MarkAndPurgeLastEntry(const Entry& entry) {
  // We need to keep mutex for all of this method
  // since end of vector is also modified by NewEntry
  absl::MutexLock lock(&entries_mutex_);

  // Locate entry
  if (index_entries_.empty()) {
    LOG(ERROR) << "Unable to purge last entry since there is none! ";
    return false;  // not found
  }

  if (index_entries_.back().filename.compare(entry.filename) != 0) {
    LOG(ERROR) << "Failed to mark entry " << entry.filename
               << " as purged, newest entry is: "
               << index_entries_.back().filename;
    monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_MARK_ENTRY_AS_PURGED);
    return false;  // not found
  }

  if (index_entries_.back().is_purged) {
    LOG(ERROR) << "Failed to mark entry " << entry.filename
               << " as purged, it is already marked!";
    return false;
  }

  // Modify
  index_entries_.back().is_purged = true;

  // Write this to disk
  if (!RewriteIndex(index_entries_)) {
    LOG(ERROR) << "Failed to purge mark entry "
               << entry.filename
               << ", rewrite of index file failed!";
    monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_MARK_ENTRY_AS_PURGED);
    return false;
  }

  // Now remove it from in memory copy.
  index_entries_.erase(index_entries_.end()-1, index_entries_.end());

  // And finally write that to disk.
  return RewriteIndex(index_entries_);
}

// Get total size of all binlogs in index
size_t BinlogIndex::GetTotalSize() const {
  absl::MutexLock lock(&entries_mutex_);
  size_t sum = 0;
  for (const Entry& entry : index_entries_) {
    if (entry.is_purged)
      continue;
    if (entry.is_closed == false)
      continue;
    sum += entry.file_size;
  }
  return sum;
}

}  // namespace mysql_ripple
