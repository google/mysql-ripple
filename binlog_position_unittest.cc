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

#include "binlog_position.h"

#include "gtest/gtest.h"
#include "buffer.h"
#include "gtid.h"
#include "log_event.h"
#include "monitoring.h"
#include "mysql_constants.h"

namespace mysql_ripple {

class TestEvent : public RawLogEventData {
 public:
  TestEvent() {
    LogEventHeader tmp;
    memset(&tmp, 0, sizeof(tmp));
    buffer_.Append(reinterpret_cast<uint8_t*>(&tmp), sizeof(tmp));
  }

  TestEvent(const TestEvent& other) {
    header = other.header;
    buffer_ = other.buffer_;
    Finalize();
  }

  TestEvent& operator=(const TestEvent& other) {
    header = other.header;
    buffer_ = other.buffer_;
    return Finalize();
  }

  TestEvent& Finalize() {
    uint8_t *ptr = buffer_.data();
    event_buffer = ptr;
    header.event_length = buffer_.size();
    header.SerializeToBuffer(ptr, sizeof(LogEventHeader));
    event_data = ptr + sizeof(LogEventHeader);
    event_data_length = buffer_.size() - sizeof(LogEventHeader);
    return *this;
  }

  uint8_t* data() {
    return buffer_.data() + sizeof(LogEventHeader);
  }

  int size() {
    return buffer_.size() - sizeof(LogEventHeader);
  }

  static TestEvent CreateGTIDEvent(int seq_no, bool stand_alone) {
    TestEvent ev;
    ev.header.type = constants::ET_GTID_MARIADB;
    ev.header.server_id = 1;
    ev.buffer_.Append(13);
    ev.Finalize();

    GTIDEvent gtid;
    gtid.gtid.server_id.assign(1);
    gtid.gtid.seq_no = seq_no;
    gtid.flags = 0;
    gtid.is_standalone = stand_alone;
    gtid.has_group_commit_id = false;
    gtid.SerializeToBuffer(ev.data(), ev.size());

    return ev;
  }

  static TestEvent CreateQueryEvent(bool commit) {
    // ripple cares about COMMIT...
    TestEvent ev;
    ev.header.type = constants::ET_QUERY;
    ev.buffer_.Append(20);
    ev.Finalize();
    if (commit) {
      QueryEvent qe;
      qe.query.assign("COMMIT");
      EXPECT_TRUE(qe.SerializeToBuffer(ev.data(), ev.size()));
    }
    return ev;
  }

  static TestEvent CreateXIDEvent() {
    TestEvent ev;
    ev.header.type = constants::ET_XID;
    ev.buffer_.Append(8);
    ev.Finalize();
    XIDEvent xid;
    xid.SerializeToBuffer(ev.data(), ev.size());
    return ev;
  }

  static TestEvent CreateRotateEvent(off_t off, const std::string& filename) {
    TestEvent ev;
    ev.header.type = constants::ET_ROTATE;
    ev.buffer_.Append(8 + filename.size());
    ev.Finalize();

    RotateEvent rotate;
    rotate.offset = off;
    rotate.filename = filename;
    rotate.SerializeToBuffer(ev.data(), ev.size());
    return ev;
  }

  static TestEvent CreateFormatDescriptor(const std::string& version,
                                          int checksum) {
    TestEvent ev;
    ev.header.type = constants::ET_FORMAT_DESCRIPTION;
    ev.buffer_.Append(100);
    ev.Finalize();

    FormatDescriptorEvent format;
    format.binlog_version = 3;
    format.server_version = version;
    format.create_timestamp = 12;
    format.checksum = checksum;
    format.SerializeToBuffer(ev.data(), ev.size());
    return ev;
  }

 private:
  Buffer buffer_;
};

TEST(BinlogPosition, DefaultPosition) {
  monitoring::Initialize();
  BinlogPosition pos;
  EXPECT_EQ(pos.group_state, BinlogPosition::NO_GROUP);
  // For code coverage...
  printf("pos.ToString().c_str(): %s\n", pos.ToString().c_str());
}

TEST(BinlogPosition, Update_group_state) {
  monitoring::Initialize();
  BinlogPosition pos;

  off_t off = 0;
  int seq_no = 1;
  EXPECT_EQ(pos.Update(TestEvent::CreateGTIDEvent(seq_no, true), off), 0);
  EXPECT_EQ(pos.group_state, BinlogPosition::STANDALONE);
  EXPECT_EQ(pos.Update(TestEvent::CreateQueryEvent(false), off), 1);
  EXPECT_EQ(pos.group_state, BinlogPosition::NO_GROUP);

  // Test that group_state remains IN_TRANSACTION
  // if opening GTIDEvent is not standalone, until XIDEvent commits it.
  seq_no++;
  EXPECT_EQ(pos.Update(TestEvent::CreateGTIDEvent(seq_no, false), off), 0);
  EXPECT_EQ(pos.group_state, BinlogPosition::IN_TRANSACTION);
  EXPECT_EQ(pos.Update(TestEvent::CreateQueryEvent(false), off), 0);
  EXPECT_EQ(pos.group_state, BinlogPosition::IN_TRANSACTION);
  EXPECT_EQ(pos.Update(TestEvent::CreateQueryEvent(false), off), 0);
  EXPECT_EQ(pos.group_state, BinlogPosition::IN_TRANSACTION);
  EXPECT_EQ(pos.Update(TestEvent::CreateXIDEvent(), off), 1);
  EXPECT_EQ(pos.group_state, BinlogPosition::NO_GROUP);

  // Test that COMMIT ends a IN_TRANSACTION
  // if opening GTIDEvent is not standalone, until XIDEvent commits it.
  seq_no++;
  EXPECT_EQ(pos.Update(TestEvent::CreateGTIDEvent(seq_no, false), off), 0);
  EXPECT_EQ(pos.group_state, BinlogPosition::IN_TRANSACTION);
  EXPECT_EQ(pos.Update(TestEvent::CreateQueryEvent(false), off), 0);
  EXPECT_EQ(pos.group_state, BinlogPosition::IN_TRANSACTION);
  EXPECT_EQ(pos.Update(TestEvent::CreateQueryEvent(true), off), 1);
  EXPECT_EQ(pos.group_state, BinlogPosition::NO_GROUP);

  seq_no++;
  {
    std::vector<TestEvent> incorrect_events;
    incorrect_events.push_back(TestEvent::CreateGTIDEvent(seq_no, true));
    incorrect_events.push_back(TestEvent::CreateGTIDEvent(seq_no, false));
    incorrect_events.push_back(TestEvent::CreateRotateEvent(0, "test"));
    incorrect_events.push_back(TestEvent::CreateFormatDescriptor("ver", 0));

    // Test that various events are not accepted while in IN_TRANSACTION-state
    {
      BinlogPosition copy = pos;
      EXPECT_EQ(copy.Update(TestEvent::CreateGTIDEvent(seq_no, false), off), 0);
      EXPECT_EQ(copy.group_state, BinlogPosition::IN_TRANSACTION);

      for (TestEvent ev : incorrect_events) {
        // NOTE: need to make copy, BinlogPosition is in undefined state
        // after failed Update
        BinlogPosition test = copy;
        EXPECT_EQ(test.Update(ev, off), -1);
      }
    }

    // Test that various events are not accepted while in STANDALONE-state
    {
      BinlogPosition copy = pos;
      EXPECT_EQ(copy.Update(TestEvent::CreateGTIDEvent(seq_no, true), off), 0);
      EXPECT_EQ(copy.group_state, BinlogPosition::STANDALONE);

      seq_no++;
      for (TestEvent ev : incorrect_events) {
        // NOTE: need to make copy, BinlogPosition is in undefined state
        // after failed Update
        BinlogPosition test = copy;
        EXPECT_EQ(test.Update(ev, off), -1);
      }
    }
  }
}

}  // namespace mysql_ripple
