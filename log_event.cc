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

#include "log_event.h"

#include "byte_order.h"
#include "mysql_constants.h"

namespace mysql_ripple {

int LogEventHeader::PackLength() const {
  return constants::LOG_EVENT_HEADER_LENGTH;
}

bool LogEventHeader::ParseFromBuffer(const uint8_t *buffer, int len) {
  if (len < constants::LOG_EVENT_HEADER_LENGTH) {
    return false;
  }
  timestamp    = byte_order::load4(buffer);
  type         = byte_order::load1(buffer + 4);
  server_id    = byte_order::load4(buffer + 5);
  event_length = byte_order::load4(buffer + 9);
  nextpos      = byte_order::load4(buffer + 13);
  flags        = byte_order::load2(buffer + 17);
  return true;
}

bool LogEventHeader::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len < constants::LOG_EVENT_HEADER_LENGTH) {
    return false;
  }
  byte_order::store4(buffer + 0, timestamp);
  byte_order::store1(buffer + 4, type);
  byte_order::store4(buffer + 5, server_id);
  byte_order::store4(buffer + 9, event_length);
  byte_order::store4(buffer + 13, nextpos);
  byte_order::store2(buffer + 17, flags);
  return true;
}

std::string LogEventHeader::ToInfoString() const {
  return "Header";
}

bool RawLogEventData::ParseFromBuffer(const uint8_t *buffer,
                                      int buffer_length) {
  header.ParseFromBuffer(buffer, buffer_length);
  event_buffer = buffer;
  event_data = event_buffer + constants::LOG_EVENT_HEADER_LENGTH;
  event_data_length = header.event_length - constants::LOG_EVENT_HEADER_LENGTH;
  return header.event_length <= buffer_length;
}

bool RawLogEventData::SerializeToBuffer(Buffer *buffer) {
  uint8_t *ptr = buffer->Append(header.event_length);
  event_buffer = ptr;
  event_data = ptr + constants::LOG_EVENT_HEADER_LENGTH;
  event_data_length = header.event_length - constants::LOG_EVENT_HEADER_LENGTH;
  header.SerializeToBuffer(ptr, constants::LOG_EVENT_HEADER_LENGTH);
  return true;
}

RawLogEventData RawLogEventData::DeepCopy(Buffer *dst) {
  // first copy header
  RawLogEventData copy = *this;

  // then copy data
  uint8_t *ptr = dst->Append(header.event_length);
  memcpy(ptr, event_buffer, header.event_length);

  // and finally setup buffer pointers
  copy.event_buffer = ptr;
  copy.event_data = copy.event_buffer + constants::LOG_EVENT_HEADER_LENGTH;
  copy.event_data_length = event_data_length;

  return copy;
}

std::string RawLogEventData::ToString() const {
  std::string tmp = "[ ";
  tmp += constants::ToString(static_cast<constants::EventType>(header.type));
  tmp += " len = " + std::to_string(header.event_length);
  tmp += " ]";

  return tmp;
}

template<typename T> std::string ParseToInfoString(RawLogEventData event) {
  T t;
  if (t.ParseFromRawLogEventData(event)) {
    return t.ToInfoString();
  }
  return "parse error";
}

std::string RawLogEventData::ToInfoString() const {
  switch (header.type) {
    case constants::ET_QUERY:
      return ParseToInfoString<QueryEvent>(*this);
    case constants::ET_ROTATE:
      return ParseToInfoString<RotateEvent>(*this);
    case constants::ET_FORMAT_DESCRIPTION:
      return ParseToInfoString<FormatDescriptorEvent>(*this);
    case constants::ET_XID:
      return ParseToInfoString<XIDEvent>(*this);
    case constants::ET_GTID_MARIADB:
      return ParseToInfoString<GTIDEvent>(*this);
    case constants::ET_START_ENCRYPTION:
      return ParseToInfoString<StartEncryptionEvent>(*this);
    case constants::ET_GTID_MYSQL:
      return ParseToInfoString<GTIDMySQLEvent>(*this);
  }
  return "";
}

EventBase::~EventBase() {
}

bool EventBase::ParseFromRawLogEventData(const RawLogEventData& event) {
  return ParseFromBuffer(event.event_data, event.event_data_length);
}

FormatDescriptorEvent::FormatDescriptorEvent() :
    binlog_version(0), create_timestamp(0), checksum(0) {
}

FormatDescriptorEvent::~FormatDescriptorEvent() {
}

void FormatDescriptorEvent::SetToRipple(const char *version) {
  server_version = version;
  binlog_version = 4;
  event_header_length = constants::LOG_EVENT_HEADER_LENGTH;
  event_type_header_lengths.resize(constants::ET_MAX);
  memset(event_type_header_lengths.data(), 0, constants::ET_MAX);

  for (auto& event : constants::NonZeroEventLengths) {
    constants::EventType event_type = event.first;
    constants::EventLength event_length = event.second;
    event_type_header_lengths[event_type-1] = event_length;
  }
}

void FormatDescriptorEvent::Reset() {
  binlog_version = 0;
  create_timestamp = 0;
  checksum = 0;
  server_version.clear();
}

bool FormatDescriptorEvent::IsEmpty() const {
  if (binlog_version != 0)
    return false;
  if (create_timestamp != 0)
    return false;
  if (!server_version.empty())
    return false;
  if (checksum != 0)
    return false;
  return true;
}

bool FormatDescriptorEvent::EqualExceptTimestamp(
    const FormatDescriptorEvent &other) const {
  return binlog_version == other.binlog_version &&
         server_version.compare(other.server_version) == 0 &&
         checksum == other.checksum;
}

int FormatDescriptorEvent::GetEventType() const {
  return constants::ET_FORMAT_DESCRIPTION;
}

int FormatDescriptorEvent::PackLength() const {
  return 2 + kServerVersionStringLen + 4 + 1 +
    event_type_header_lengths.size() + 1;
}

bool FormatDescriptorEvent::ParseFromBuffer(const uint8_t *buffer, int len) {
  // the format descriptor event consists of some fixed size parts
  // and then a variable section and finally the checksum TRUE/FALSE in the end.
  // we don't (currently) care about the variable section.
  const int fixed_len = 2 + kServerVersionStringLen + 4 + 1;
  if (len < fixed_len) {
    return false;
  }

  binlog_version = byte_order::load2(buffer);
  const char *ptr = reinterpret_cast<const char*>(buffer + 2);
  size_t version_len = strnlen(ptr, kServerVersionStringLen);
  server_version = std::string(ptr, version_len);
  create_timestamp = byte_order::load4(buffer + kServerVersionStringLen + 2);
  event_header_length = byte_order::load1(buffer + kServerVersionStringLen +
                                          2 + 4);
  for (uint offset = kServerVersionStringLen + 2 + 4 + 1; offset < len - 1;
       offset++) {
    event_type_header_lengths.push_back(byte_order::load1(buffer + offset));
  }
  checksum = byte_order::load1(buffer + len - 1);
  return true;
}

bool FormatDescriptorEvent::SerializeToBuffer(uint8_t *buffer, int len) const {
  // the format descriptor event consists of some fixed size parts
  // and then a variable section and finally the checksum TRUE/FALSE in the end.
  // we don't (currently) care about the variable section.
  if (len < PackLength())
    return false;

  size_t version_len = server_version.size();
  if (version_len > kServerVersionStringLen) {
    return false;
  }

  byte_order::store2(buffer, binlog_version);
  memset(buffer + 2, 0, kServerVersionStringLen);
  server_version.copy(reinterpret_cast<char*>(buffer + 2),
                      kServerVersionStringLen);
  byte_order::store4(buffer + kServerVersionStringLen + 2, create_timestamp);
  byte_order::store1(buffer + kServerVersionStringLen + 2 + 4,
                     event_header_length);
  for (uint i = 0; i < event_type_header_lengths.size(); i++) {
    uint offset = i + kServerVersionStringLen + 2 + 4 + 1;
    byte_order::store1(buffer + offset, event_type_header_lengths[i]);
  }
  byte_order::store1(buffer + len - 1, checksum);
  return true;
}

std::string FormatDescriptorEvent::ToInfoString() const {
  return
      "Server ver: " + server_version +
      ", Binlog ver: " + std::to_string(binlog_version) +
      ", event header length: " + std::to_string(event_header_length) +
      ", event_type_header_lengths: " +
      std::to_string(event_type_header_lengths.size()) +
      ", checksum: " + std::to_string(checksum);
}

int RotateEvent::GetEventType() const {
  return constants::ET_ROTATE;
}

int RotateEvent::PackLength() const {
  return 8 + filename.size();
}

bool RotateEvent::ParseFromBuffer(const uint8_t *buffer, int len) {
  if (len < 8) {
    return false;
  }

  offset = byte_order::load8(buffer);
  filename.assign(reinterpret_cast<const char*>(buffer + 8), len - 8);
  return true;
}

bool RotateEvent::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len < PackLength())
    return false;

  byte_order::store8(buffer, offset);
  memcpy(buffer + 8, filename.c_str(), filename.size());  // not \0 terminated!
  return true;
}

std::string RotateEvent::ToInfoString() const {
  return filename + ":" + std::to_string(offset);
}

int GTIDEvent::GetEventType() const {
  return constants::ET_GTID_MARIADB;
}

int GTIDEvent::PackLength() const {
  return 13 + (has_group_commit_id ? 8 : 0);
}

bool GTIDEvent::ParseFromBuffer(const uint8_t *buffer, int len) {
  if (len < 13)
    return false;
  // gtid.server_id stored in LogEventHeader
  gtid.seq_no = byte_order::load8(buffer);
  gtid.domain_id = byte_order::load4(buffer + 8);
  flags = byte_order::load1(buffer + 12);

  has_group_commit_id = (flags & HAS_GROUP_COMMIT_ID) ? 1 : 0;
  is_standalone = (flags & IS_STANDALONE) ? 1 : 0;
  if (has_group_commit_id) {
    if (len < 21)
      return false;
    commit_no = byte_order::load8(buffer + 13);
  }

  return true;
}

bool GTIDEvent::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len < PackLength())
    return false;

  byte_order::store8(buffer + 0, gtid.seq_no);
  byte_order::store4(buffer + 8, gtid.domain_id);

  int tmp = flags;
  if (is_standalone)
    tmp |= IS_STANDALONE;
  if (has_group_commit_id)
    tmp |= HAS_GROUP_COMMIT_ID;

  byte_order::store1(buffer + 12, tmp);

  if (has_group_commit_id) {
    if (len < 21)
      return false;
    byte_order::store8(buffer + 13, commit_no);
  }

  return true;
}

std::string GTIDEvent::ToInfoString() const {
  return
      (is_standalone ? std::string("GTID ") : std::string("BEGIN GTID ")) +
      std::to_string(gtid.domain_id) + "-" +
      std::to_string(gtid.server_id.server_id) + "-" +
      std::to_string(gtid.seq_no);
}

int GTIDMySQLEvent::GetEventType() const {
  return constants::ET_GTID_MYSQL;
}

int GTIDMySQLEvent::PackLength() const {
  return 1 + Uuid::PACK_LENGTH + 8;
}

bool GTIDMySQLEvent::ParseFromBuffer(const uint8_t *buffer, int len) {
  if (len < PackLength())
    return false;

  if (buffer[0] & 1) {
    commit_flag = true;
  } else {
    commit_flag = false;
  }

  gtid.server_id.uuid.ParseFromBuffer(buffer + 1, Uuid::PACK_LENGTH);
  gtid.server_id.server_id = 0;
  gtid.seq_no = byte_order::load8(buffer + 1 + Uuid::PACK_LENGTH);
  gtid.domain_id = 0;

  return true;
}

bool GTIDMySQLEvent::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len < PackLength())
    return false;

  if (commit_flag) {
    byte_order::store1(buffer + 0, 1);
  } else {
    byte_order::store1(buffer + 0, 0);
  }
  gtid.server_id.uuid.SerializeToBuffer(buffer + 1, Uuid::PACK_LENGTH);
  byte_order::store8(buffer + 1 + Uuid::PACK_LENGTH, gtid.seq_no);

  return true;
}

std::string GTIDMySQLEvent::ToInfoString() const {
  return
      (commit_flag ? std::string("GTID ") : std::string("BEGIN GTID ")) +
      gtid.server_id.uuid.ToString() + ":" +
      std::to_string(gtid.seq_no);
}

int QueryEvent::GetEventType() const {
  return constants::ET_QUERY;
}

int QueryEvent::PackLength() const {
  uint8_t db_len = 0;
  uint16_t status_len = 0;
  return 13 + db_len + status_len + 1 + query.size();
}

bool QueryEvent::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len < PackLength())
    return false;

  // these are not yet used
  uint32_t thread_id = 0;
  uint32_t exec_time = 0;
  uint8_t db_len = 0;
  uint16_t error_code = 0;
  uint16_t status_len = 0;

  const uint8_t *end = buffer + len;
  byte_order::store4(buffer + 0, thread_id);
  byte_order::store4(buffer + 4, exec_time);
  byte_order::store1(buffer + 8, db_len);
  byte_order::store2(buffer + 9, error_code);
  byte_order::store2(buffer + 11, status_len);

  buffer += 13 + status_len + db_len;
  if (buffer >= end) {
    return false;
  }

  byte_order::store1(buffer + 0, 0);  // should be 0 byte here
  buffer++;  // skip over \0

  int remain = end - buffer;
  if (remain < query.size()) {
    return false;
  }
  memset(buffer, 0, remain);
  query.copy(reinterpret_cast<char*>(buffer), remain);
  return true;
}

bool QueryEvent::ParseFromBuffer(const uint8_t *buffer, int len) {
  if (len < 13)
    return false;

  const uint8_t *end = buffer + len;
  // uint32_t thread_id = byte_order::load4(buffer + 0);
  // uint32_t exec_time = byte_order::load4(buffer + 4);
  uint8_t db_len = byte_order::load1(buffer + 8);
  // uint16_t error_code = byte_order::load2(buffer + 9);
  uint16_t status_len = byte_order::load2(buffer+ 11);

  buffer += 13 + status_len + db_len;
  if (buffer >= end)
    return false;

  if (buffer[0] != 0)
    return false;

  buffer++;  // skip over \0

  // query text is stored in the rest
  query.assign(reinterpret_cast<const char*>(buffer), end - buffer);
  return true;
}

std::string QueryEvent::ToInfoString() const {
  return query;
}

int XIDEvent::GetEventType() const {
  return constants::ET_XID;
}

int XIDEvent::PackLength() const {
  return 8;
}

bool XIDEvent::ParseFromBuffer(const uint8_t *buffer, int len) {
  if (len < 8)
    return false;
  memcpy(&xid, buffer, 8);
  return true;
}

bool XIDEvent::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len < PackLength())
    return false;
  memcpy(buffer, &xid, 8);
  return true;
}

std::string XIDEvent::ToInfoString() const {
  return "COMMIT /* xid=" + std::to_string(xid) + " */";
}

int StartEncryptionEvent::GetEventType() const {
  return constants::ET_START_ENCRYPTION;
}

int StartEncryptionEvent::PackLength() const {
  return 24;  // crypt_scheme + key_version + nonce
}

bool StartEncryptionEvent::ParseFromBuffer(const uint8_t* ptr, int len) {
  if (len < PackLength())
    return false;

  crypt_scheme = byte_order::load4(ptr + 0);
  key_version = byte_order::load4(ptr + 4);
  nonce.assign(reinterpret_cast<const char*>(ptr + 8), 16);
  return true;
}

bool StartEncryptionEvent::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len < PackLength())
    return false;

  byte_order::store4(buffer + 0, crypt_scheme);
  byte_order::store4(buffer + 4, key_version);
  nonce.copy(reinterpret_cast<char*>(buffer + 8), 16);
  return true;
}

std::string StartEncryptionEvent::ToInfoString() const {
  return
      "crypt_scheme=" + std::to_string(crypt_scheme) + " " +
      "key_version=" + std::to_string(key_version);
}

int HeartbeatEvent::GetEventType() const {
  return constants::ET_HEARTBEAT;
}

int HeartbeatEvent::PackLength() const {
  // a heartbeat consists of filename + offset,
  // but the offset is stored in LogEventHeader
  return filename.size();
}

bool HeartbeatEvent::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len < PackLength())
    return false;

  memcpy(buffer, filename.data(), len);
  return true;
}

bool HeartbeatEvent::ParseFromBuffer(const uint8_t *ptr, int len) {
  // not implemented.
  return false;
}

std::string HeartbeatEvent::ToInfoString() const {
  return "filename=" + filename;
}

}  // namespace mysql_ripple
