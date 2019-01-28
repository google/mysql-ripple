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

#ifndef MYSQL_RIPPLE_MANAGER_H
#define MYSQL_RIPPLE_MANAGER_H

#include <string>

#include "management.pb.h"
#include "mysql_master_session.h"
#include "session_factory.h"

namespace mysql_ripple {

class Manager {
 public:
  class RippledInterface {
   public:
    virtual ~RippledInterface() {}
    virtual mysql::MasterSession &GetMasterSession() const = 0;
    virtual mysql::SlaveSessionFactory &GetSlaveSessionFactory() const = 0;
    virtual BinlogPosition GetBinlogPosition() const = 0;
    virtual bool StartMasterSession(std::string *, bool) = 0;
    virtual bool StopMasterSession(std::string *, bool) = 0;
    virtual std::string GetServerName() const = 0;
  };

  Manager(RippledInterface *rippled) : rippled_(rippled) {}

  void FillRippleInfo(ripple_proto::RippleInfo *info);
  void FillMasterStatus(ripple_proto::MasterStatus *master_status);
  void FillConnectedSlaves(ripple_proto::Slaves *slaves);
  void FillSlaveBinlogPosition(const ripple_proto::SlaveAddress& address,
                               ripple_proto::BinlogPosition *position);
  void FillBinlogPosition(ripple_proto::BinlogPosition *position);

  void ToggleSemiSyncReply(const ripple_proto::OnOff *onoff,
                           ripple_proto::Status *status);

  void StartSlave(ripple_proto::Status *status,
                  const ripple_proto::StartSlaveRequest *request);
  void StopSlave(ripple_proto::Status *status,
                 const ripple_proto::StopSlaveRequest *request);

  void ChangeMaster(const ripple_proto::MasterInfo *master_info,
                    ripple_proto::Status *status);

 private:
  RippledInterface *rippled_;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MANAGER_H
