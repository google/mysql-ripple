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

#include "flags.h"

namespace mysql_ripple {

DEFINE_string(ripple_rw_rpc_users, "",
              "List of users allowed to make rpcs that alter "
              "configuration/behaviour of ripple (empty means anyone).");

DEFINE_int32(ripple_server_id, 112211,
             "Server id used by ripple");

DEFINE_string(ripple_server_uuid, "",
              "Server uuid used by ripple. "
              "If not specified, one is constructed from server_id");

DEFINE_string(ripple_server_ports, "51005",
              "Comma separated list of ports"
              " that ripple will bind for incoming connections");

DEFINE_string(ripple_server_address, "localhost",
              "Interface that ripple will bind for incoming connections");

DEFINE_string(ripple_server_type, "tcp",
              "Server type that ripple will expose for incoming connections");

DEFINE_string(ripple_server_name, "",
              "Server name that ripple sets when connecting to master "
              "and exposes in the server_name variable");

DEFINE_string(ripple_master_user, "root",
              "Connect to master using this user");

DEFINE_string(ripple_master_protocol, "",
              "Connect to master using this protcol "
              "(if none set using mysql default)");

DEFINE_int32(ripple_master_port, 51001,
             "Port that ripple will use to connect to master");

DEFINE_string(ripple_master_address, "",
              "Address that ripple will use to connect to master");

DEFINE_bool(ripple_master_compressed_protocol, true,
            "Shall ripple use compressed protocol when connecting to master");

DEFINE_double(ripple_master_heartbeat_period, 0.1,  // 100ms
              "Heartbeat period used for master connection");

DEFINE_int32(ripple_encryption_scheme, 255,  // encryption with fake key server
             "Encryption scheme used by ripple for local binlogs");

DEFINE_string(ripple_datadir, ".",
              "Directory in which ripple will save local binlogs");

DEFINE_int32(ripple_max_binlog_size, 1073741824,
             "Size after which binlog is rotated");

DEFINE_bool(danger_danger_use_dbug_keys, false,
            "Use dbug keys (compatible with mysqld)");

DEFINE_string(ripple_version_protocol, "5.6.0-ripple",
              "Version string returned in authentication packet");

DEFINE_string(ripple_version_binlog, "5.6.0-ripple",
              "Version string stored in binlog");

DEFINE_int32(ripple_master_reconnect_period, 2,
             "Limit reconnect attempts to"
             " ripple_master_reconnect_attempts per"
             " ripple_master_reconnect_period seconds");
DEFINE_int32(ripple_master_reconnect_attempts, 3,
             "Limit reconnect attempts to"
             " ripple_master_reconnect_attempts per"
             " ripple_master_reconnect_period");

DEFINE_bool(ripple_semi_sync_slave_enabled, false,
            "Shall ripple send semi-sync acks to the master");

DEFINE_string(ripple_requested_start_gtid_position, "",
              "Use this GTID as requested start position if no"
              " gtids are found on disk");

DEFINE_int32(ripple_purge_expire_logs_days, 0,
             "Purge binlog files older than this days (0=disable)."
             " This is evaluated independently of"
             " ripple_purge_keep_size.");

DEFINE_uint64(ripple_purge_logs_keep_size, 0,
              "Purge binlog files if total size exeeds this value (0=disable)."
              " This is evaluated independently of"
              " ripple_purge_expire_logs_days.");

DEFINE_int32(ripple_master_alloc_server_id_timeout, 1000,
             "Wait for maximum this ms when making sure that server id is"
             " unique when connecting to a master.");

DEFINE_int32(ripple_slave_alloc_server_id_timeout, 5000,
             "Wait for maximum this ms when making sure that server id is"
             " unique when a slave connects.");

}  // namespace mysql_ripple
