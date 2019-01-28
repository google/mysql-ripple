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

#ifndef MYSQL_RIPPLE_MYSQL_CONSTANTS_H
#define MYSQL_RIPPLE_MYSQL_CONSTANTS_H

#include <cstdint>
#include <map>
#include <string>

namespace mysql_ripple {

namespace constants {

static const int LOG_EVENT_HEADER_LENGTH = 19;
static const char BINLOG_HEADER[4] = {(char) 0xfe, 0x62, 0x69, 0x6e};

enum EventType {
  // Event types that ripple cares/knows about.
  ET_QUERY = 2,
  ET_STOP = 3,
  ET_ROTATE = 4,
  ET_FORMAT_DESCRIPTION = 15,
  ET_XID = 16,
  ET_INCIDENT = 26,
  ET_HEARTBEAT = 27,
  ET_GTID_MYSQL = 33,
  ET_PREVIOUS_GTIDS_MYSQL = 35,
  ET_START_ENCRYPTION = 150,
  ET_BINLOG_CHECKPOINT = 161,
  ET_GTID_MARIADB = 162,
  ET_GTID_LIST_MARIADB = 163,
  ET_MAX = ET_GTID_LIST_MARIADB,

  // Event types that ripple uses to construct an FD for the first binlog file:
  ET_START = 1,
  ET_LOAD = 6,
  ET_CREATE_FILE = 8,
  ET_APPEND_BLOCK = 9,
  ET_EXEC_LOAD = 10,
  ET_DELETE_FILE = 11,
  ET_NEW_LOAD = 12,
  ET_BEGIN_LOAD_QUERY = 17,
  ET_EXECUTE_LOAD_QUERY = 18,
  ET_TABLE_MAP = 19,
};

std::string ToString(EventType);

// Binlog v4 event lengths for ripple to construct a FormatDescriptorEvent.
// These lengths were taken from mysql documentation and/or MariaDB source
// code.
enum EventLength {
  EL_START = 56,
  EL_QUERY = 13,
  EL_ROTATE = 8,
  EL_LOAD = 18,
  EL_CREATE_FILE = 4,
  EL_APPEND_BLOCK = 4,
  EL_EXEC_LOAD = 4,
  EL_DELETE_FILE = 4,
  EL_NEW_LOAD = 18,
  EL_FORMAT_DESCRIPTION = 84,
  EL_BEGIN_LOAD_QUERY = 4,
  EL_EXECUTE_LOAD_QUERY = 26,
  EL_TABLE_MAP = 8,
  EL_INCIDENT = 2,
  EL_BINLOG_CHECKPOINT = 4,
  EL_GTID_MARIADB = 19,
  EL_GTID_LIST_MARIADB = 4
};

static const std::map<EventType, EventLength> NonZeroEventLengths = {
  { ET_START, EL_START},
  { ET_QUERY, EL_QUERY},
  { ET_ROTATE, EL_ROTATE},
  { ET_LOAD, EL_LOAD},
  { ET_CREATE_FILE, EL_CREATE_FILE},
  { ET_APPEND_BLOCK, EL_APPEND_BLOCK},
  { ET_EXEC_LOAD, EL_EXEC_LOAD},
  { ET_DELETE_FILE, EL_DELETE_FILE},
  { ET_NEW_LOAD, EL_NEW_LOAD},
  { ET_FORMAT_DESCRIPTION, EL_FORMAT_DESCRIPTION},
  { ET_BEGIN_LOAD_QUERY, EL_BEGIN_LOAD_QUERY},
  { ET_EXECUTE_LOAD_QUERY, EL_EXECUTE_LOAD_QUERY},
  { ET_TABLE_MAP, EL_TABLE_MAP},
  { ET_INCIDENT, EL_INCIDENT},
  { ET_BINLOG_CHECKPOINT, EL_BINLOG_CHECKPOINT},
  { ET_GTID_MARIADB, EL_GTID_MARIADB},
  { ET_GTID_LIST_MARIADB, EL_GTID_LIST_MARIADB}
};

enum MariaDB {
  // We support GTID and holes.
  MARIADB_SLAVE_CAPABILITY = 4
};

// Connection capabilities that are negotiated during
// client/server handshake. Can be found in include/mysql_com.h.
enum Capabilities {
  CAPABILITY_NONE                   = 0,
  CAPABILITY_LONG_PASSWORD          = uint32_t(1) << 0,
  CAPABILITY_FOUND_ROWS             = uint32_t(1) << 1,
  CAPABILITY_LONG_FLAG              = uint32_t(1) << 2,
  CAPABILITY_CONNECT_WITH_DB        = uint32_t(1) << 3,
  CAPABILITY_NO_SCHEMA              = uint32_t(1) << 4,
  CAPABILITY_COMPRESS               = uint32_t(1) << 5,
  CAPABILITY_ODBC                   = uint32_t(1) << 6,
  CAPABILITY_LOCAL_FILES            = uint32_t(1) << 7,
  CAPABILITY_IGNORE_SPACE           = uint32_t(1) << 8,
  CAPABILITY_PROTOCOL_41            = uint32_t(1) << 9,
  CAPABILITY_INTERACTIVE            = uint32_t(1) << 10,
  CAPABILITY_SSL                    = uint32_t(1) << 11,
  CAPABILITY_IGNORE_SIGPIPE         = uint32_t(1) << 12,
  CAPABILITY_TRANSACTIONS           = uint32_t(1) << 13,
  CAPABILITY_RESERVED               = uint32_t(1) << 14,
  CAPABILITY_SECURE_CONNECTION      = uint32_t(1) << 15,
  CAPABILITY_MULTI_STATEMENTS       = uint32_t(1) << 16,
  CAPABILITY_MULTI_RESULTS          = uint32_t(1) << 17,
  CAPABILITY_PS_MULTI_RESULTS       = uint32_t(1) << 18,
  CAPABILITY_PLUGIN_AUTH            = uint32_t(1) << 19,
  CAPABILITY_CONNECT_ATTRS          = uint32_t(1) << 20,
  CAPABILITY_PLUGIN_AUTH_LENENC     = uint32_t(1) << 21,
  CAPABILITY_EXPIRED_PASSWORDS      = uint32_t(1) << 22,
  CAPABILITY_CLIENT_PROGRESS        = uint32_t(1) << 29,
  CAPABILITY_SSL_VERIFY_SERVER_CERT = uint32_t(1) << 30,
  CAPABILITY_REMEMBER_OPTIONS       = uint32_t(1) << 31
};

// Commands, can be found in include/mysql_com.h.
enum Commands {
  COM_SLEEP               = 0,
  COM_QUIT                = 1,
  COM_INIT_DB             = 2,
  COM_QUERY               = 3,
  COM_FIELD_LIST          = 4,
  COM_CREATE_DB           = 5,
  COM_DROP_DB             = 6,
  COM_REFRESH             = 7,
  COM_SHUTDOWN            = 8,
  COM_STATISTICS          = 9,
  COM_PROCESS_INFO        = 10,
  COM_CONNECT             = 11,
  COM_PROCESS_KILL        = 12,
  COM_DEBUG               = 13,
  COM_PING                = 14,
  COM_TIME                = 15,
  COM_DELAYED_INSERT      = 16,
  COM_CHANGE_USER         = 17,
  COM_BINLOG_DUMP         = 18,
  COM_TABLE_DUMP          = 19,
  COM_CONNECT_OUT         = 20,
  COM_REGISTER_SLAVE      = 21,
  COM_STMT_PREPARE        = 22,
  COM_STMT_EXECUTE        = 23,
  COM_STMT_SEND_LONG_DATA = 24,
  COM_STMT_CLOSE          = 25,
  COM_STMT_RESET          = 26,
  COM_SET_OPTION          = 27,
  COM_STMT_FETCH          = 28,
  COM_DAEMON              = 29,
  COM_BINLOG_DUMP_GTID    = 30 /* used by Oracle MySQL */
};

// Data types, can be found in include/mysql_com.h.
enum DataType {
  TYPE_LONG = 0x03,
  TYPE_LONGLONG = 0x08,
  TYPE_VARCHAR = 0x0f
};

// Reply packet magic numbers
enum ReplyPacket {
  OK_PACKET = 0x0,
  EOF_PACKET = 0xfe,
  ERR_PACKET = 0xff
};

// Result set magic numbers
enum ResultSet {
  nullptr_COLUMN = 0xfb
};

// Semi sync
enum SemiSync {
  SEMI_SYNC_HEADER = 0xEF,

  SEMI_SYNC_REPLY = 0x01,
  SEMI_SYNC_NSLB_REPLY = 0x02
};

}  // namespace constants

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_CONSTANTS_H
