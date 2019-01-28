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

#ifndef MYSQL_RIPPLE_FLAGS_H
#define MYSQL_RIPPLE_FLAGS_H

#include "gflags/gflags.h"

namespace mysql_ripple {

DECLARE_string(ripple_rw_rpc_users);

DECLARE_string(ripple_server_ports);
DECLARE_string(ripple_server_address);
DECLARE_string(ripple_server_type);

DECLARE_int32(ripple_master_port);
DECLARE_string(ripple_master_user);
DECLARE_string(ripple_master_protocol);
DECLARE_string(ripple_master_address);
DECLARE_bool(ripple_master_compressed_protocol);
DECLARE_double(ripple_master_heartbeat_period);

DECLARE_int32(ripple_server_id);
DECLARE_string(ripple_server_uuid);
DECLARE_string(ripple_server_name);

DECLARE_int32(ripple_encryption_scheme);

DECLARE_string(ripple_datadir);
DECLARE_int32(ripple_max_binlog_size);

DECLARE_bool(danger_danger_use_dbug_keys);

DECLARE_string(ripple_version_protocol);
DECLARE_string(ripple_version_binlog);

DECLARE_int32(ripple_master_reconnect_period);
DECLARE_int32(ripple_master_reconnect_attempts);

DECLARE_bool(ripple_semi_sync_slave_enabled);

DECLARE_string(ripple_requested_start_gtid_position);

DECLARE_int32(ripple_purge_expire_logs_days);
DECLARE_uint64(ripple_purge_logs_keep_size);

DECLARE_int32(ripple_master_alloc_server_id_timeout);
DECLARE_int32(ripple_slave_alloc_server_id_timeout);

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_FLAGS_H
