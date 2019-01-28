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

#ifndef MYSQL_RIPPLE_LOG_EVENT_H
#define MYSQL_RIPPLE_LOG_EVENT_H

#include <sys/types.h>

#include <cstdint>
#include <string>

#include "buffer.h"
#include "gtid.h"

namespace mysql_ripple {

// This is log event header.
struct LogEventHeader {
  uint32_t timestamp;
  uint8_t type;
  uint32_t server_id;
  uint32_t event_length;
  uint32_t nextpos;
  uint16_t flags;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int PackLength() const;
  bool ParseFromBuffer(const uint8_t *buffer, int len);
  bool SerializeToBuffer(uint8_t *buffer, int len) const;
};

// This is a raw log event, header is parsed but not the data.
struct RawLogEventData {
  LogEventHeader header;

  // Pointer to event.
  const uint8_t *event_buffer;

  // Length of event data (i.e excluding event header).
  int event_data_length;

  // Pointer to event data.
  const uint8_t *event_data;

  bool ParseFromBuffer(const uint8_t *buffer, int buffer_length);
  bool SerializeToBuffer(Buffer *buffer);

  // Copy RawLogEventData header+data into dst and return a RawLogEventData
  // pointing to it.
  RawLogEventData DeepCopy(Buffer *dst);

  std::string ToString() const;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;
};

// Base class for LogEvents.
// Created so that one can parse/serialize wo/ knowing
// which exact event it is.
struct EventBase {
  virtual ~EventBase();
  virtual int GetEventType() const = 0;
  virtual bool ParseFromBuffer(const uint8_t *buffer, int len) = 0;
  virtual int PackLength() const = 0;
  virtual bool SerializeToBuffer(uint8_t *buffer, int len) const = 0;

  virtual bool ParseFromRawLogEventData(const RawLogEventData& event);
};

// Serialize into a Buffer
template <typename T> bool SerializeToBuffer(const T& ev, Buffer *dst) {
  int size = ev.PackLength();
  uint8_t *ptr = dst->Append(size);
  return ev.SerializeToBuffer(ptr, size);
}

// Parse from Buffer
template <typename T> bool ParseFromBuffer(T *ev, const Buffer& src) {
  return ev->ParseFromBuffer(src.data(), src.size());
}

// Parse from RawLogEventData
template <typename T>
bool ParseFromRawLogEventData(T *ev, const RawLogEventData &src) {
  return ev->ParseFromBuffer(src.event_data, src.event_data_length);
}

struct FormatDescriptorEvent : public EventBase {
  uint16_t binlog_version;
  std::string server_version;
  uint32_t create_timestamp;

  uint8_t event_header_length;
  std::vector<uint8_t> event_type_header_lengths;

  uint8_t checksum;

  // the size of the server version string in the serialized log event.
  static const int kServerVersionStringLen = 50;

  FormatDescriptorEvent();
  virtual ~FormatDescriptorEvent();

  // Use this to fill out an empty format descriptor event when writing the
  // first FD to the binlog.
  void SetToRipple(const char *version);

  void Reset();
  bool IsEmpty() const;
  bool EqualExceptTimestamp(const FormatDescriptorEvent& other) const;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int GetEventType() const override;
  bool ParseFromBuffer(const uint8_t *buffer, int len) override;
  int PackLength() const override;
  bool SerializeToBuffer(uint8_t *buffer, int len) const override;
};

struct RotateEvent : public EventBase {
  std::string filename;
  off_t offset;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int GetEventType() const override;
  bool ParseFromBuffer(const uint8_t *buffer, int len) override;
  int PackLength() const override;
  bool SerializeToBuffer(uint8_t *buffer, int len) const override;
};

struct GTIDEvent : public EventBase {
  GTID gtid;
  int flags;
  uint64_t commit_no;

  enum Flags {
    IS_STANDALONE = 1,
    HAS_GROUP_COMMIT_ID = 2
  };

  // unpacked flags
  uint8_t has_group_commit_id;
  uint8_t is_standalone;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int GetEventType() const override;

  // NOTE: The data of the GTIDEvent is not self contained
  // hence care must be taken when using these methods.
  bool ParseFromBuffer(const uint8_t *buffer, int len) override;
  int PackLength() const override;
  bool SerializeToBuffer(uint8_t *buffer, int len) const override;

  bool ParseFromRawLogEventData(const RawLogEventData& event) override {
    if (EventBase::ParseFromRawLogEventData(event)) {
      // The data of the GTIDEvent is not self contained,
      // as the server id is not stored in the data part
      // but is taken from the LogEventHeader.
      gtid.server_id.assign(event.header.server_id);
      return true;
    }
    return false;
  }
};

struct GTIDMySQLEvent : public EventBase {
  GTID gtid;

  // unpacked flags
  uint8_t commit_flag;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int GetEventType() const override;

  bool ParseFromBuffer(const uint8_t *buffer, int len) override;
  int PackLength() const override;
  bool SerializeToBuffer(uint8_t *buffer, int len) const override;
};

// A QueryEvent.
// We don't really care about query events...
//
// except that in SBR a transaction like
// BEGIN-GTID; DML that does not affect any rows; Commit
// is ended with a QueryEvent(COMMIT) instead of a XIDEvent. So to keep track
// of group boundaries we also parse QueryEvent to see if they are COMMIT
struct QueryEvent : public EventBase {
  std::string query;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int GetEventType() const override;
  bool ParseFromBuffer(const uint8_t *buffer, int len) override;
  int PackLength() const override;
  bool SerializeToBuffer(uint8_t *buffer, int len) const override;
};

struct XIDEvent : public EventBase {
  uint64_t xid;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int GetEventType() const override;
  bool ParseFromBuffer(const uint8_t *buffer, int len) override;
  int PackLength() const override;
  bool SerializeToBuffer(uint8_t *buffer, int len) const override;
};

struct StartEncryptionEvent : public EventBase {
  uint32_t crypt_scheme;
  uint32_t key_version;
  std::string nonce;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int GetEventType() const override;
  bool ParseFromBuffer(const uint8_t *ptr, int len) override;
  int PackLength() const override;
  bool SerializeToBuffer(uint8_t *buffer, int len) const override;
};

struct HeartbeatEvent : public EventBase {
  std::string filename;

  // String used by SHOW BINLOG EVENTS;
  std::string ToInfoString() const;

  int GetEventType() const override;
  bool ParseFromBuffer(const uint8_t *ptr, int len) override;
  int PackLength() const override;
  bool SerializeToBuffer(uint8_t *buffer, int len) const override;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_LOG_EVENT_H
