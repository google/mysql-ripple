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

#ifndef MYSQL_RIPPLE_MYSQL_PROTOCOL_H
#define MYSQL_RIPPLE_MYSQL_PROTOCOL_H

#include <cstdint>

#include "mysql_server_connection.h"
#include "log_event.h"
#include "resultset.h"

namespace mysql_ripple {

namespace mysql {

// This struct represents arguments sent in COM_Binlog_Dump_GTID
// that is sent by Oracle MySQL
struct COM_Binlog_Dump_GTID
{
  uint16_t flags;
  uint32_t server_id;
  std::string filename;
  uint32_t position;
  GTIDSet gtid_executed;
};

class Protocol {
 public:
  explicit Protocol(ServerConnection *con) :
      event_checksums_(false), connection_(con) {}
  virtual ~Protocol() {}

  virtual void SetEventChecksums(bool val) { event_checksums_ = val; }
  virtual bool GetEventChecksums() const { return event_checksums_; }

  virtual bool Authenticate();
  virtual bool ValidateNativeHash(const char *authdata, int authdata_len,
                                  const char *slave_pwhash,
                                  unsigned char *scramble);
  virtual bool SendAuthSwitchRequest(const char *plugin, unsigned char *data,
                                     size_t data_len);
  virtual bool SendOK();
  virtual bool SendEOF();
  virtual bool SendERR(int code, const char* sqlstate, const char* msg);
  virtual bool SendResultset(const resultset::Resultset& resultset);
  virtual bool SendMetaData(const resultset::ColumnDefinition columns[],
                            int count);
  virtual bool SendResultRow(const resultset::ColumnData columns[],
                             int count);
  virtual bool SendEvent(RawLogEventData event);

  // Compute bytes needed to store number.
  int PackLength(uint64_t number);
  // Compute bytes needed to store null terminated string.
  int PackLength(const char *str);

  // Store number, return end of data
  uint8_t* Pack(uint8_t *ptr, uint64_t number);
  // Store null terminated string, return end of data
  uint8_t* Pack(uint8_t *ptr, const char *str);

  uint64_t Unpack(const uint8_t **ptr);
  template <typename T> inline uint64_t Unpack(const T **ptr) {
    return Unpack(reinterpret_cast<const uint8_t**>(ptr));
  }

  static uint32_t ComputeEventChecksum(const uint8_t *ptr, int length);
  static bool VerifyAndStripEventChecksum(RawLogEventData *event);
  static void StripEventChecksum(RawLogEventData *event);

  static bool Unpack(const uint8_t *ptr, int length, COM_Binlog_Dump_GTID *dst);
  static void Pack(const COM_Binlog_Dump_GTID& src, Buffer *dst);

 private:
  bool event_checksums_;
  ServerConnection *connection_;  // not owned
};

}  // namespace mysql

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_PROTCOL_H
