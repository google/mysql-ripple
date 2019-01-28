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

#ifndef MYSQL_RIPPLE_MYSQL_LISTENER_H
#define MYSQL_RIPPLE_MYSQL_LISTENER_H

#include "mysql_server_port.h"
#include "session.h"
#include "session_factory.h"

namespace mysql_ripple {

// A threads listening to a port
class Listener : public ThreadedSession {
 public:
  explicit Listener(SessionFactory *factory,
                    mysql::ServerPort *port);
  virtual ~Listener();

  // Override Stop() to wakeup listener hanging in Accept()
  bool Stop() override;

 private:
  SessionFactory *factory_;
  mysql::ServerPort *port_;

 protected:
  void* Run();

  Listener(Listener&&) = delete;
  Listener(const Listener&) = delete;
  Listener& operator=(Listener&&) = delete;
  Listener& operator=(const Listener&) = delete;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_LISTENER_H
