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

#include "mysql_protocol.h"

#include <string.h>
#include <zlib.h>

// MySQL client library includes
#include "mysql.h"
#include "mysqld_error.h"

#include "buffer.h"
#include "byte_order.h"
#include "flags.h"
#include "logging.h"

namespace mysql_ripple {

namespace mysql {

bool Protocol::Authenticate() {
  /**
     Packet format:

     Bytes       Content
     -----       ----
     1           protocol version (always 10)
     n           server version string, \0-terminated
     4           thread id
     8           first 8 bytes of the plugin provided data (scramble)
     1           \0 byte, terminating the first part of a scramble
     2           server capabilities (two lower bytes)
     1           server character set
     2           server status
     2           server capabilities (two upper bytes)
     1           length of the scramble
     10          reserved, always 0
     m           rest of the plugin provided data (at least 12 bytes)
     1           \0 byte, terminating the second part of a scramble
     p           name of plugin

     sum: 33 + n + m + p
  */
  const char plugin[] = "mysql_native_password";
  const char *version = FLAGS_ripple_version_protocol.c_str();
  size_t version_len = strlen(version) + 1;

  // TODO(jonaso): random data as scramble
  char scramble[SCRAMBLE_LENGTH] = { 0 };

  int thread_id = 28;
  uint16_t status = 0;
  uint32_t own_capabilities =
      constants::CAPABILITY_COMPRESS |
      constants::CAPABILITY_LONG_PASSWORD |
      constants::CAPABILITY_PROTOCOL_41 |
      constants::CAPABILITY_SECURE_CONNECTION |
      constants::CAPABILITY_PLUGIN_AUTH |
      constants::CAPABILITY_PLUGIN_AUTH_LENENC;

  Buffer packet;
  uint8_t *ptr = packet.Append(33 +
                               version_len +
                               SCRAMBLE_LENGTH - 8 +
                               sizeof(plugin));

  // protocol version (always 10)
  *ptr = 10;
  ptr++;

  // server version string, \0-terminated
  memcpy(ptr, version, version_len);
  ptr += version_len;

  // connection id
  byte_order::store4(ptr, thread_id);
  ptr += 4;

  // first 8 byte of scramble
  memcpy(ptr, scramble, 8);
  ptr += 8;

  // \0 terminate first part of scramble
  *ptr = 0;
  ptr += 1;

  // server capabilities (two lower bytes)
  byte_order::store2(ptr, own_capabilities & 0xFFFF);
  ptr += 2;

  // charset
  *ptr = 0;
  ptr += 1;

  // server status
  byte_order::store2(ptr, status);
  ptr += 2;

  // server capabilities (two upper bytes)
  byte_order::store2(ptr, own_capabilities >> 16);
  ptr += 2;

  // length of the scramble, including 0 byte
  *ptr = SCRAMBLE_LENGTH + 1;
  ptr += 1;

  // reserved, always 0
  memset(ptr, 0, 10);  // 10 zeros
  ptr += 10;

  // rest of scramble
  memcpy(ptr, scramble + 8, SCRAMBLE_LENGTH - 8);
  ptr += SCRAMBLE_LENGTH - 8;

  // \0 terminate last part of scramble
  *ptr = 0;
  ptr += 1;

  // plugin name
  memcpy(ptr, plugin, sizeof(plugin));
  ptr += sizeof(plugin);

  Connection::Packet p = {
    static_cast<int>(ptr - packet.data()), packet.data() };
  assert(p.length <= packet.size());
  if (!connection_->WritePacket(p)) {
    LOG(WARNING) << "Failed to write auth packet";
    return false;
  }

  p = connection_->ReadPacket();
  if (p.length == -1) {
    LOG(WARNING) << "Failed to read response to auth packet";
    return false;
  }

  int charset_no = 0;
  uint32_t capabilities = byte_order::load2(p.ptr);
  if ((capabilities & constants::CAPABILITY_PROTOCOL_41) == 0) {
    return false;
  }

  // protocol41 uses >= 32 bytes
  if (p.length < 32) {
    return false;
  } else {
    uint32_t capabilities_hi = byte_order::load2(p.ptr + 2);
    capabilities |= capabilities_hi << 16;
  }

  if (capabilities & constants::CAPABILITY_SSL) {
    LOG(WARNING) << "Client requests SSL...which is not supported";
    return false;
  }

  if ((capabilities & constants::CAPABILITY_SECURE_CONNECTION) == 0) {
    return false;
  }

  if ((capabilities & constants::CAPABILITY_PLUGIN_AUTH_LENENC) == 0) {
    return false;
  }

  bool compress = (capabilities & constants::CAPABILITY_COMPRESS) != 0;

  // uint32_t max_packet_length = byte_order::load4(p.ptr + 4);
  charset_no = p.ptr[8];

  const uint8_t *end = p.ptr + 32;
  const int remain = p.length - 32;

  const char *user = reinterpret_cast<const char*>(end);
  const int user_len = strnlen(user, remain);
  if (user_len == remain) {
    // there should atleast be a \0 byte as password
    return false;
  }
  const char *passwd = user + user_len + 1;
  int passwd_len = Unpack(&passwd);  // length encoded

  // TODO(jonaso): Validate password
  // TODO(crystall): Add monitoring for authentication failures when jonaso
  // validates password.
  printf("user: %.*s password(%d): %.*s\n", user_len, user,
         passwd_len, passwd_len, passwd);

  if (SendOK()) {
    connection_->Reset();
    connection_->SetCompressed(compress);
    return true;
  }

  return false;
}

bool Protocol::SendOK() {
  uint8_t packet[8];
  uint8_t *ptr = packet;

  // OK
  *ptr = constants::OK_PACKET;
  ptr += 1;

  // affected rows
  int rows = 0;
  ptr = Pack(ptr, rows);

  // last insert id
  int last_insert_id = 0;
  ptr = Pack(ptr, last_insert_id);

  // status
  uint16_t status = 0;
  byte_order::store2(ptr, status);
  ptr += 2;

  // warnings
  uint16_t warnings = 0;
  byte_order::store2(ptr, warnings);
  ptr += 2;

  // info
  *ptr = 0;
  ptr++;

  Connection::Packet p = { static_cast<int>(ptr - packet), packet };
  assert(p.length <= sizeof(packet));
  if (!connection_->WritePacket(p)) {
    LOG(WARNING) << "Failed to write OK packet";
    return false;
  }

  return true;
}

bool Protocol::SendEOF() {
  uint8_t packet[5];
  uint8_t *ptr = packet;

  // OK
  *ptr = constants::EOF_PACKET;
  ptr += 1;

  // warnings
  uint16_t warnings = 0;
  byte_order::store2(ptr, warnings);
  ptr += 2;

  // status
  uint16_t status = 0;
  byte_order::store2(ptr, status);
  ptr += 2;

  Connection::Packet p = { static_cast<int>(ptr - packet), packet };
  assert(p.length <= sizeof(packet));
  if (!connection_->WritePacket(p)) {
    LOG(WARNING) << "Failed to write EOF packet";
    return false;
  }

  return true;
}

bool Protocol::SendERR(int code, const char *sqlstate, const char *msg) {
  uint8_t packet[1 + 2 + 1 + 5];
  uint8_t *ptr = packet;

  // ERR
  ptr[0] = constants::ERR_PACKET;

  // code
  byte_order::store2(ptr + 1, code);

  // #
  ptr[3] = '#';

  // sql-state
  memcpy(ptr + 4, sqlstate, 5);

  Buffer buffer;
  buffer.Append(packet, sizeof(packet));
  buffer.Append(reinterpret_cast<const uint8_t*>(msg), strlen(msg));
  if (!connection_->WritePacket(buffer)) {
    LOG(WARNING) << "Failed to write ERR packet";
    return false;
  }

  return true;
}

bool Protocol::SendMetaData(
    const resultset::ColumnDefinition cols[],
    int count) {

  Buffer buffer;
  // field count
  buffer.resize(PackLength(count));

  Pack(buffer.data(), count);
  if (!connection_->WritePacket(buffer))
    return false;

  for (int i = 0; i < count; i++) {
    const resultset::ColumnDefinition *col = cols + i;
    int length = 0;
    length += PackLength("def");
    length += PackLength(col->schema);
    length += 2 * PackLength(col->table);
    length += 2 * PackLength(col->name);
    length += 1 + 12;
    buffer.resize(length);

    uint8_t *ptr = buffer.data();
    ptr = Pack(ptr, "def");
    ptr = Pack(ptr, col->schema);
    ptr = Pack(ptr, col->table);
    ptr = Pack(ptr, col->table);
    ptr = Pack(ptr, col->name);
    ptr = Pack(ptr, col->name);
    byte_order::store1(ptr + 0, 12);  // length
    byte_order::store2(ptr + 1, 0);   // charset
    byte_order::store4(ptr + 3, col->max_length);
    byte_order::store1(ptr + 7, col->datatype);
    byte_order::store2(ptr + 8, 0);   // flags
    byte_order::store1(ptr + 9, 0);   // decimals
    byte_order::store2(ptr + 10, 0);  // filler
    if (!connection_->WritePacket(buffer)) {
      return false;
    }
  }

  return SendEOF();
}

bool Protocol::SendResultRow(
    const resultset::ColumnData cols[],
    int count) {

  Buffer buffer;
  for (int i = 0; i < count; i++) {
    if (cols[i].data == nullptr) {
      uint8_t packet[1] = { constants::nullptr_COLUMN };
      if (!buffer.Append(packet+0, sizeof(packet)))
        return false;
    } else {
      uint8_t *ptr = buffer.Append(PackLength(cols[i].data));
      if (ptr == nullptr)
        return false;
      Pack(ptr, cols[i].data);
    }
  }
  if (!connection_->WritePacket(buffer))
    return false;

  return true;
}

bool Protocol::SendResultset(const resultset::Resultset& result) {
  if (result.column_definition.empty() && result.rows.empty()) {
    if (!SendOK())
      return false;
    return true;
  }

  if (!SendMetaData(&result.column_definition[0],
                    result.column_definition.size()))
    return false;

  for (int i = 0; i < result.rows.size(); i++) {
    if (!SendResultRow(&result.rows[i].at(0),
                       result.rows[i].size()))
      return false;
  }

  if (!SendEOF())
    return false;

  return true;
}

bool Protocol::SendEvent(RawLogEventData log_event) {
  // These are sent "as is"
  Buffer b;
  uint8_t *ptr = b.Append(log_event.header.event_length + 1 +
                          (event_checksums_ ? 4 : 0));
  ptr[0] = 0;
  memcpy(ptr + 1, log_event.event_buffer, log_event.header.event_length);
  if (event_checksums_) {
    // reserialize the header since length includes checksum
    log_event.header.event_length += 4;
    log_event.header.SerializeToBuffer(ptr + 1,
                                       constants::LOG_EVENT_HEADER_LENGTH);

    uint32_t val = ComputeEventChecksum(ptr + 1,
                                        log_event.header.event_length - 4);
    byte_order::store4(ptr + 1 + log_event.header.event_length - 4, val);
  }

  if (!connection_->WritePacket(b)) {
    LOG(ERROR) << "Failed to send event: "
               << connection_->GetLastErrorMessage()
               << ", type: " << constants::ToString(
                   static_cast<constants::EventType>(log_event.header.type))
               << ", length: " << log_event.header.event_length;
    return false;
  }

  if (log_event.header.type == constants::ET_HEARTBEAT) {
    // don't spam log with these...
    DLOG_EVERY_N(INFO, 10)
        << "Send event"
        << ", type: " << constants::ToString(
            static_cast<constants::EventType>(log_event.header.type))
        << ", timestamp: " << log_event.header.timestamp
        << ", length: " << log_event.header.event_length
        << ", nextpos: " << log_event.header.nextpos;
  } else {
    DLOG(INFO) << "Send event"
               << ", type: " << constants::ToString(
                   static_cast<constants::EventType>(log_event.header.type))
               << ", timestamp: " << log_event.header.timestamp
               << ", length: " << log_event.header.event_length
               << ", nextpos: " << log_event.header.nextpos;
  }

  return true;
}

uint32_t Protocol::ComputeEventChecksum(const uint8_t *ptr, int length) {
  return crc32(0, ptr, length);
}

bool Protocol::VerifyAndStripEventChecksum(RawLogEventData *event) {
  if (event->header.event_length < constants::LOG_EVENT_HEADER_LENGTH + 4) {
    LOG(WARNING) << "Failed to verify event checksum"
                 << " - header.event_length: " << event->header.event_length
                 << " too short";
    return false;
  }
  uint32_t computed = ComputeEventChecksum(event->event_buffer,
                                           event->header.event_length - 4);
  uint32_t stored = byte_order::load4(event->event_buffer +
                                      event->header.event_length - 4);

  if (computed == stored) {
    StripEventChecksum(event);
    return true;
  }

  LOG(WARNING) << "Verification of checksum failed!!"
               << ", computed: " << computed
               << ", stored: " << stored
               << ", type: " << constants::ToString(
                   static_cast<constants::EventType>(event->header.type))
               << ", length: " << event->header.event_length;

  return false;
}

void Protocol::StripEventChecksum(RawLogEventData *event) {
  event->header.event_length -= 4;
  event->event_data_length -= 4;
}

int Protocol::PackLength(uint64_t val) {
  if (val < 251) {
    return 1;
  }
  if (val < 65536) {
    return 3;
  }
  if (val < 16777216) {
    return 4;
  }
  return 9;
}

uint8_t* Protocol::Pack(uint8_t *ptr, uint64_t val) {
  if (val < 251) {
    *ptr = static_cast<uint8_t>(val);
    return ptr + 1;
  }
  if (val < 65536) {
    *ptr = 252;
    byte_order::store2(ptr + 1, val);
    return ptr + 3;
  }
  if (val < 16777216) {
    *ptr = 253;
    byte_order::store3(ptr + 1, val);
    return ptr + 4;
  }
  *ptr = 254;
  byte_order::store8(ptr + 1, val);
  return ptr + 9;
}

uint64_t Protocol::Unpack(const uint8_t **ptr) {
  const uint8_t *tmp = *ptr;
  if (*tmp < 251) {
    *ptr = tmp + 1;
    return *tmp;
  }
  if (*tmp == 252) {
    *ptr = tmp + 3;
    return byte_order::load2(tmp + 1);
  }
  if (*tmp == 253) {
    *ptr = tmp + 4;
    return byte_order::load3(tmp + 1);
  }

  assert(*tmp == 254);
  *ptr = tmp + 9;
  return byte_order::load8(tmp + 1);
}

int Protocol::PackLength(const char *str) {
  size_t len = str == nullptr ? 0 : strlen(str);
  return PackLength(len) + len;
}

uint8_t* Protocol::Pack(uint8_t *ptr, const char *str) {
  size_t len = str == nullptr ? 0 : strlen(str);
  ptr = Pack(ptr, len);
  if (str != nullptr) {
    memcpy(ptr, str, len);
    ptr += len;
  }
  return ptr;
}

bool Protocol::Unpack(const uint8_t *ptr, int length,
                      COM_Binlog_Dump_GTID *dst) {
  if (length < 10)
    return false;
  dst->flags = byte_order::load2(ptr + 0);
  dst->server_id = byte_order::load4(ptr + 2);
  uint32_t name_size = byte_order::load4(ptr + 6);
  if (length < 10 + name_size + 8 + 4)
    return false;
  dst->filename.assign(reinterpret_cast<const char*>(ptr + 10), name_size);
  dst->position = byte_order::load8(ptr + 10 + name_size);
  uint32_t data_size = byte_order::load4(ptr + 10 + name_size + 8);
  if (length < 10 + name_size + 8 + 4 + data_size)
    return false;
  return dst->gtid_executed.ParseFromBuffer(ptr + 10 + name_size + 8 + 4,
                                            data_size);
}

void Protocol::Pack(const COM_Binlog_Dump_GTID& src, Buffer *dst) {
  int name_len = src.filename.length();
  int gtid_len = src.gtid_executed.PackLength();
  int len = 2 + 4 + 4 + name_len + 8 + 4 + gtid_len;
  uint8_t *ptr = dst->Append(len);

  byte_order::store2(ptr + 0, src.flags);
  byte_order::store4(ptr + 2, src.server_id);
  byte_order::store4(ptr + 6, name_len);
  memcpy(ptr + 10, src.filename.data(), name_len);
  byte_order::store8(ptr + 10 + name_len, src.position);
  byte_order::store4(ptr + 10 + name_len + 8, gtid_len);
  src.gtid_executed.SerializeToBuffer(ptr + 10 + name_len + 8 + 4,
                                      gtid_len);
}

}  // namespace mysql

}  // namespace mysql_ripple
