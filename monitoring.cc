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

#include "monitoring.h"
#include "absl/strings/string_view.h"

namespace monitoring {
CallbackMetric<std::string>* master_connection_status;
CallbackMetric<std::string>* last_master_connect_error;
CallbackMetric<uint64_t>* time_since_master_last_connected;
Metric<bool>* rippled_active;
Metric<uint64_t>* bytes_sent_to_master;
Metric<uint64_t>* bytes_received_from_master;

Metric<uint32_t, std::string>* slave_current_event_timestamp;
CallbackMetric<std::string, std::string>* slave_connection_status;
CallbackMetric<std::string, std::string>* last_slave_connect_error;
Metric<uint>* num_slaves;
Metric<uint64_t, std::string>* bytes_sent_to_slave;
Metric<uint64_t, std::string>* bytes_received_from_slave;

Counter<std::string>* rippled_binlog_error;
// Note about how this metric is used in binlog.cc: Most events are written
// to the Binlog via WriteEvent, and we set this metric's value there.
// However, FormatDescriptorEvents are written to the Binlog in custom
// functions that rebuild FormatDescriptors from fields and discard the event
// timestamps, so we must log these events from Binlog::AddEvent directly
// while their timestamps are still valid.
Metric<uint32_t>* binlog_last_event_timestamp;
Metric<uint64_t>* binlog_last_event_received;

void Initialize() {
  bytes_sent_to_master = new Metric<uint64_t>();
  bytes_received_from_master = new Metric<uint64_t>();
  bytes_sent_to_slave = new Metric<uint64_t, std::string>();
  bytes_received_from_slave = new Metric<uint64_t, std::string>();
  master_connection_status = new CallbackMetric<std::string>();
  last_master_connect_error = new CallbackMetric<std::string>();
  time_since_master_last_connected = new CallbackMetric<uint64_t>();
  rippled_active = new Metric<bool>();
  slave_current_event_timestamp = new Metric<uint32_t, std::string>();
  slave_connection_status = new CallbackMetric<std::string, std::string>();
  last_slave_connect_error = new CallbackMetric<std::string, std::string>();
  num_slaves = new Metric<uint>();
  rippled_binlog_error = new Counter<std::string>();
  binlog_last_event_timestamp = new Metric<uint32_t>();
  binlog_last_event_received = new Metric<uint64_t>();
}

}   // namespace monitoring
