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

#include "mysql_server_port.h"

#include "gtest/gtest.h"
#include "executor.h"
#include "init.h"
#include "plugin.h"

namespace mysql_ripple {

const char *kPortType = "tcp";

class Acceptor : public RunnableInterface {
 public:
  explicit Acceptor(mysql::ServerPort& port) : port_(port) {}
  virtual ~Acceptor() {}

  void Run() {
    printf("before Accept()\n");
    port_.Accept();
    printf("after Accept()\n");
  }

  void Stop() {
    printf("before Shutdown\n");
    port_.Shutdown();
    printf("after Shutdown\n");
  }

  void Unref() {
    delete this;
  }

 private:
  mysql::ServerPort& port_;
};

TEST(ServerPort, ShutdownAccept) {
  ThreadPoolExecutor pool;
  std::unique_ptr<mysql::ServerPort> port(
      mysql::ServerPortFactory::GetInstance(kPortType, "localhost", "0"));
  EXPECT_TRUE(port->Bind());
  EXPECT_TRUE(port->Listen());
  EXPECT_TRUE(port->Shutdown());
  EXPECT_TRUE(pool.Execute(new Acceptor(*port)));
  // test will hang here unless the Accept() call has been interrupted
  // by the Shutdown call.
  pool.WaitStopped();
  EXPECT_TRUE(port->Close());
}

TEST(ServerPort, AcceptShutdown) {
  ThreadPoolExecutor pool;
  std::unique_ptr<mysql::ServerPort> port(
      mysql::ServerPortFactory::GetInstance(kPortType, "localhost", "0"));
  EXPECT_TRUE(port->Bind());
  EXPECT_TRUE(port->Listen());
  EXPECT_TRUE(pool.Execute(new Acceptor(*port)));
  // sleep(1) to make it almost certain that the accept-call has been made
  // before calling Shutdown.
  sleep(1);
  EXPECT_TRUE(port->Shutdown());
  // test will hang here unless the Accept() call has been interrupted
  // by the Shutdown call.
  pool.WaitStopped();
  EXPECT_TRUE(port->Close());
}

}  // namespace mysql_ripple

namespace mysql_ripple {

namespace plugin {

extern Plugin plugin_mysql_server_port_tcpip;

}  // namespace plugin

}  // namespace mysql_ripple


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  mysql_ripple::Init(argc, argv);
  mysql_ripple::plugin::plugin_mysql_server_port_tcpip.Init();

  return RUN_ALL_TESTS();
}
