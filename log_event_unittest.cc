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

#include "log_event.h"

#include "gtest/gtest.h"
#include "buffer.h"
#include "encryption.h"
#include "mysql_constants.h"

namespace mysql_ripple {

class TestEventFactory {
 public:
  TestEventFactory(KeyHandler *key_handler) : key_handler_(key_handler) {}

  void Create(LogEventHeader *dst) {
    dst->timestamp    = time(0);
    dst->type         = (time(0) & 255);
    dst->server_id    = time(0);
    dst->event_length = time(0);
    dst->nextpos      = time(0);
    dst->flags        = time(0) & 65536;
  }

  void Create(GTIDEvent *dst) {
    dst->gtid.server_id.assign(1);
    dst->gtid.seq_no = time(0);
    dst->flags = 0;
    dst->is_standalone = (time(0) & 1) ? true : false;
    dst->has_group_commit_id = false;
  }

  void Create(XIDEvent *ev) {
    ev->xid = time(0);
  }

  void Create(RotateEvent *dst) {
    dst->offset = time(0);
    dst->filename = "thisisafilename.bin";
  }

  void Create(FormatDescriptorEvent *dst) {
    dst->binlog_version = 3;
    dst->server_version = "4.3.2-test-version";
    dst->create_timestamp = 12;
    dst->checksum = (time(0) & 1) ? true : false;
    dst->event_header_length = constants::LOG_EVENT_HEADER_LENGTH;
  }

  void Create(QueryEvent *dst) {
    dst->query = "COMMIT";
  }

  void Create(StartEncryptionEvent *dst) {
    Buffer nonce;
    key_handler_->GetRandomBytes(nonce.data(), nonce.size());

    dst->crypt_scheme = 1;
    dst->key_version = key_handler_->GetLatestKeyVersion();
    dst->nonce.assign(reinterpret_cast<const char*>(nonce.data()),
                      nonce.size());
  }

 private:
  std::unique_ptr<KeyHandler> key_handler_;
};

// Test that serialize, parse, serialize gives identical byte sequence
template <typename T> void TestSerializeAndParse(TestEventFactory *factory) {
  T ev1;
  factory->Create(&ev1);
  Buffer buf1;
  SerializeToBuffer<T>(ev1, &buf1);
  T ev2;
  ParseFromBuffer(&ev2, buf1);
  Buffer buf2;
  SerializeToBuffer<T>(ev2, &buf2);
  EXPECT_FALSE(ev1.SerializeToBuffer(nullptr, 0));
  {
    Buffer too_small_buffer;
    too_small_buffer.Append(ev1.PackLength() - 1);
    EXPECT_FALSE(ev1.SerializeToBuffer(too_small_buffer.data(),
                                       too_small_buffer.size()));

    T ev3;
    EXPECT_FALSE(ev3.ParseFromBuffer(nullptr, 0));
    EXPECT_FALSE(ev2.SerializeToBuffer(nullptr, 0));
  }
  EXPECT_EQ(buf1.size(), buf2.size());
  EXPECT_EQ(memcmp(buf1.data(), buf2.data(),
                   std::min(buf1.size(), buf2.size())), 0);
  printf("Info: %s\n", ev1.ToInfoString().c_str());
}

TEST(LogEvent, Basic) {
  TestEventFactory factory(KeyHandler::GetInstance(true));

  TestSerializeAndParse<LogEventHeader>(&factory);
  TestSerializeAndParse<XIDEvent>(&factory);
  TestSerializeAndParse<RotateEvent>(&factory);
  TestSerializeAndParse<GTIDEvent>(&factory);
  TestSerializeAndParse<StartEncryptionEvent>(&factory);
  TestSerializeAndParse<FormatDescriptorEvent>(&factory);
  TestSerializeAndParse<QueryEvent>(&factory);

  for (int i = 0; i < 256; i++) {
    printf("%d event type: %s\n", i,
           constants::ToString(static_cast<constants::EventType>(i)).c_str());
  }
}

}  // namespace mysql_ripple
