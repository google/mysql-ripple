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

#include "gtid.h"

#include "gtest/gtest.h"
#include "mysql_compat.h"

namespace mysql_ripple {

TEST(GTID, Successor) {
  GTID g1;
  EXPECT_TRUE(g1.IsEmpty());
  EXPECT_TRUE(g1.equal(g1));

  g1.set_server_id(1).set_domain_id(1).set_sequence_no(1);
  EXPECT_TRUE(g1.equal(g1));
  EXPECT_FALSE(g1.IsEmpty());

  GTID g2(g1);
  EXPECT_TRUE(g1.equal(g2));
  EXPECT_TRUE(g2.equal(g1));

  g2.set_sequence_no(2);
  EXPECT_FALSE(g1.equal(g2));
  EXPECT_FALSE(g2.equal(g1));

  g2.set_domain_id(2);
  EXPECT_FALSE(g1.equal(g2));
  EXPECT_FALSE(g2.equal(g1));
}

TEST(GTIDStartPosition, Contained) {
  GTIDStartPosition pos1;
  EXPECT_TRUE(pos1.IsEmpty());
  GTID g;
  EXPECT_TRUE(pos1.Update(g));
  EXPECT_TRUE(pos1.IsEmpty());

  g.set_server_id(1).set_sequence_no(1);
  EXPECT_TRUE(pos1.Update(g));
  EXPECT_FALSE(pos1.IsEmpty());
  EXPECT_FALSE(pos1.Update(g));

  g.set_sequence_no(2);
  EXPECT_TRUE(pos1.Update(g));  // 0-1-2

  GTIDStartPosition pos2;
  g.set_sequence_no(10);
  EXPECT_TRUE(pos2.Update(g));  // 0-1-10

  for (int i = 1; i < 12; i++) {
    GTID g2(g);
    g2.set_sequence_no(i);
    GTIDStartPosition pos3;
    EXPECT_TRUE(pos3.Update(g2));  // 0-1-i
    printf("pos3: %s\n", pos3.ToString().c_str());
    if (i <= 2 || i > 10) {
      EXPECT_FALSE(pos3.IsContained(pos1, pos2));
    } else {
      EXPECT_TRUE(pos3.IsContained(pos1, pos2));
    }

    GTIDStartPosition empty;
    if (i <= 2) {
      EXPECT_FALSE(pos3.IsContained(pos1, empty));
    } else {
      EXPECT_TRUE(pos3.IsContained(pos1, empty));
    }

    if (i <= 10) {
      EXPECT_TRUE(pos3.IsContained(empty, pos2));
    } else {
      EXPECT_FALSE(pos3.IsContained(empty, pos2));
    }

    EXPECT_FALSE(pos3.IsContained(empty, empty));
  }
}

TEST(GTIDStartPosition, Parse) {
  std::string str;
  GTIDStartPosition pos;
  pos.ToMariaDBConnectState(&str);
  EXPECT_EQ(0, str.size());
  EXPECT_TRUE(pos.IsEmpty());
  EXPECT_TRUE(pos.ParseMariaDBConnectState(""));
  EXPECT_TRUE(pos.IsEmpty());
  EXPECT_TRUE(pos.ParseMariaDBConnectState("''"));
  EXPECT_TRUE(pos.IsEmpty());
  EXPECT_FALSE(pos.ParseMariaDBConnectState("'"));

  GTID g;
  g.set_domain_id(1).set_server_id(2).set_sequence_no(3);
  EXPECT_TRUE(pos.Update(g));
  pos.ToMariaDBConnectState(&str);
  EXPECT_EQ(0, str.compare("1-2-3"));
  EXPECT_TRUE(pos.ParseMariaDBConnectState("1-2-3"));
  pos.ToMariaDBConnectState(&str);
  EXPECT_EQ(0, str.compare("1-2-3"));
  EXPECT_TRUE(pos.ParseMariaDBConnectState("'1-2-3'"));
  pos.ToMariaDBConnectState(&str);
  EXPECT_EQ(0, str.compare("1-2-3"));

  EXPECT_FALSE(pos.ParseMariaDBConnectState("'1-2-3"));
  EXPECT_FALSE(pos.ParseMariaDBConnectState("1-2-3'"));
  EXPECT_FALSE(pos.ParseMariaDBConnectState("1-2-"));
  EXPECT_FALSE(pos.ParseMariaDBConnectState("1--3"));
  EXPECT_FALSE(pos.ParseMariaDBConnectState("-2-3"));

  EXPECT_TRUE(pos.ParseMariaDBConnectState("1-2-3,2-2-3"));
  EXPECT_FALSE(pos.ParseMariaDBConnectState("1-2-3,1-2-3"));
  EXPECT_TRUE(pos.ParseMariaDBConnectState("1-2-2,1-2-3"));
  EXPECT_FALSE(pos.ParseMariaDBConnectState("1-2-3,1-2-2"));

  // MySQL's GTID set format shouldn't be parsed successfully.
  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:1-5";
  EXPECT_FALSE(pos.ParseMariaDBConnectState(str));
  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:1-5,"
        "6c27ed6d-7ee1-11e3-be39-6c626d957cfb:10-15:20-25";
  EXPECT_FALSE(pos.ParseMariaDBConnectState(str));
}

TEST(GTIDSet, uuid) {
  Uuid uuid;
  std::string str = "6c27ed6d-7ee1-11e3-be39-6c626d957cff";
  EXPECT_TRUE(uuid.Parse(str));
  EXPECT_EQ(uuid.ToString(), str);

  char text[Uuid::TEXT_LENGTH + 1] = { 0 };
  EXPECT_TRUE(uuid.ToString(text, sizeof(text) - 1));
  EXPECT_EQ(0, uuid.ToString().compare(text));
  EXPECT_FALSE(uuid.ToString(text, 0));

  uint8_t buffer[Uuid::PACK_LENGTH];
  EXPECT_FALSE(uuid.ToString(nullptr, 0));
  EXPECT_TRUE(uuid.SerializeToBuffer(buffer, sizeof(buffer)));
  EXPECT_FALSE(uuid.SerializeToBuffer(buffer, sizeof(buffer) - 1));

  Uuid uuid2;
  std::string str2 = "6C27ED6D-7EE1-11E3-BE39-6C626D957CFF";
  EXPECT_TRUE(uuid2.Parse(str2));
  EXPECT_TRUE(uuid2.Equal(uuid));
  EXPECT_EQ(0, uuid2.ToString().compare(str));

  EXPECT_FALSE(uuid2.ParseFromBuffer(buffer, sizeof(buffer) - 1));
  EXPECT_TRUE(uuid2.ParseFromBuffer(buffer, sizeof(buffer)));
  EXPECT_TRUE(uuid.Equal(uuid2));

  EXPECT_FALSE(uuid.Parse(str.substr(0, str.length() - 1)));
  str = "6c27ed6d-7eE1-11e3-be39-6c626d9kalle";
  EXPECT_FALSE(uuid.Parse(str));
  str = "6c27ed6d=7eE1-11e3-be39-6c626d957cff";
  EXPECT_FALSE(uuid.Parse(str));
}

TEST(GTIDSet, Basic) {
  GTIDSet set;
  std::string str =
      "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:1-5"
      ",6c27ed6d-7ee1-11e3-be39-6c626d957cfb:3"
      ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23-29";
  EXPECT_TRUE(set.Parse(str));
  EXPECT_EQ(set.ToString(), str);

  int len = set.PackLength();
  uint8_t *buffer = new uint8_t[len];
  EXPECT_FALSE(set.SerializeToBuffer(buffer, len - 1));
  EXPECT_TRUE(set.SerializeToBuffer(buffer, len));

  GTIDSet set2;
  EXPECT_FALSE(set2.ParseFromBuffer(buffer, len - 1));
  EXPECT_FALSE(set2.ParseFromBuffer(buffer, 7));
  memset(buffer, 255, 4);
  EXPECT_FALSE(set2.ParseFromBuffer(buffer, len));

  EXPECT_TRUE(set.SerializeToBuffer(buffer, len));
  EXPECT_TRUE(set2.ParseFromBuffer(buffer, len));
  delete[] buffer;

  EXPECT_EQ(set.ToString(), set2.ToString());

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc";
  EXPECT_FALSE(set.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c6";
  EXPECT_FALSE(set.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23kalle-23";
  EXPECT_FALSE(set.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23-23anka";
  EXPECT_FALSE(set.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc-23-23anka";
  EXPECT_FALSE(set.Parse(str));

  str = "6c2kkk6d-7ee1-11e3-be39-6c626d957cfc:23-23";
  EXPECT_FALSE(set.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23-23";
  EXPECT_TRUE(set.Parse(str));
  EXPECT_EQ(set.ToString(), "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23");

  // MariaDB's GTID format shouldn't be parsed successfully
  str = "1-2-3";
  EXPECT_FALSE(set.Parse(str));
  str = "1-2-3,2-12345678-123";
  EXPECT_FALSE(set.Parse(str));
}

TEST(GTIDList, Parse_MariaDB) {
  GTIDList list;
  std::string str;

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:3-1-23";
  EXPECT_TRUE(list.Parse(str));
  EXPECT_TRUE(list.Parse("'" + str + "'"));
  EXPECT_EQ(list.ToString(), str);
  EXPECT_EQ(list.GetStreamKey(), GTIDList::KEY_DOMAIN_ID);
  EXPECT_EQ(list.GetListMode(), GTIDList::MODE_STRICT_MONOTONIC);

  // MariaDB can't have gaps.
  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:3-1-[1-10][15-20]";
  EXPECT_FALSE(list.Parse(str));
}

TEST(GTIDList, Parse_MySQL) {
  GTIDList list;
  std::string str;

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:0-0-[23-29][31-34]"
        ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:0-0-23";
  EXPECT_TRUE(list.Parse(str));
  EXPECT_TRUE(list.Parse("'" + str + "'"));
  EXPECT_EQ(list.GetStreamKey(), GTIDList::KEY_UUID);
  EXPECT_EQ(list.GetListMode(), GTIDList::MODE_GAPS);

  // List mode for MySQL is always MODE_GAPS, because gaps can appear at any
  // time, even if there are no gaps initially.
  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:0-0-30"
        ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:0-0-23";
  EXPECT_TRUE(list.Parse(str));
  EXPECT_TRUE(list.Parse("'" + str + "'"));
  EXPECT_EQ(list.GetStreamKey(), GTIDList::KEY_UUID);
  EXPECT_EQ(list.GetListMode(), GTIDList::MODE_GAPS);

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:0-0-5"  // 5 == [1-5]
        ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:0-0-[23-29][31-34]";
  EXPECT_TRUE(list.Parse(str));
  EXPECT_TRUE(list.Parse("'" + str + "'"));
  EXPECT_EQ(list.ToString(), str);
  EXPECT_EQ(list.GetStreamKey(), GTIDList::KEY_UUID);
  EXPECT_EQ(list.GetListMode(), GTIDList::MODE_GAPS);
  EXPECT_TRUE(list.Equal(list));

  GTIDList copy(list);
  GTID gtid;
  gtid.domain_id = 0;
  gtid.server_id.uuid.Parse("6c27ed6d-7ee1-11e3-be39-6c626d957cfc");
  gtid.server_id.server_id = 0;
  gtid.seq_no = 30;
  EXPECT_TRUE(copy.Update(gtid));
  EXPECT_FALSE(list.Equal(copy));
  EXPECT_FALSE(copy.Equal(list));
  EXPECT_TRUE(copy.Equal(copy));
  EXPECT_TRUE(GTIDList::Subset(copy, copy));
  EXPECT_TRUE(GTIDList::Subset(list, copy));
  EXPECT_FALSE(GTIDList::Subset(copy, list));
  EXPECT_EQ(copy.ToString(),
            "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:0-0-5"  // 5 == [1-5]
            ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:0-0-[23-34]");

  gtid.seq_no = 22;
  EXPECT_TRUE(copy.Update(gtid));
  gtid.seq_no = 35;
  EXPECT_TRUE(copy.Update(gtid));

  gtid.seq_no = 19;
  EXPECT_TRUE(copy.Update(gtid));
  gtid.seq_no = 20;
  EXPECT_TRUE(copy.Update(gtid));
  gtid.seq_no = 21;
  EXPECT_TRUE(copy.Update(gtid));
  gtid.seq_no = 21;
  EXPECT_FALSE(copy.Update(gtid));

  gtid.seq_no = 50;
  EXPECT_TRUE(copy.Update(gtid));

  EXPECT_FALSE(list.Equal(copy));
  EXPECT_FALSE(copy.Equal(list));
  EXPECT_TRUE(copy.Equal(copy));
  EXPECT_TRUE(GTIDList::Subset(copy, copy));
  EXPECT_TRUE(GTIDList::Subset(list, copy));
  EXPECT_FALSE(GTIDList::Subset(copy, list));
  EXPECT_EQ(copy.ToString(),
            "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:0-0-5"  // 5 == [1-5]
            ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:0-0-[19-35][50-50]");
}

TEST(GTIDList, Parse_BadSyntax) {
  GTIDList list;
  std::string str;

  str = "'6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-1-5";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:-1-5";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-xx-5";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-1-[-5]";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-1-[1xx5]";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-1-[1-]";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-1-[1-5";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-1-[]";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-1-,2-2-2";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc";
  EXPECT_FALSE(list.Parse(str));

  str = "'";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c6";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23kalle-23";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23-23anka";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc-23-23anka";
  EXPECT_FALSE(list.Parse(str));

  str = "6c2kkk6d-7ee1-11e3-be39-6c626d957cfc:23-23";
  EXPECT_FALSE(list.Parse(str));

  str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23-23";
  EXPECT_FALSE(list.Parse(str));
}

TEST(GTIDList, Parse_Empty) {
  GTIDList list;
  std::string str;

  str = "''";
  EXPECT_TRUE(list.Parse(str));

  str = "";
  EXPECT_TRUE(list.Parse(str));
}

void RandUpdate(GTIDList list, GTID gtid) {
  GTIDList copy(list);
  EXPECT_TRUE(copy.Equal(list));

  int cnt = 0;
  unsigned seed = 42;
  for (int i = 0; i < 100; i++) {
    gtid.seq_no = rand_r(&seed) % 200;
    if (list.Contained(gtid)) {
      EXPECT_FALSE(list.ValidSuccessor(gtid));
      EXPECT_FALSE(list.Update(gtid));
    } else if (list.ValidSuccessor(gtid)) {
      EXPECT_TRUE(list.Update(gtid));
      cnt++;
    }
  }
  EXPECT_TRUE(GTIDList::Subset(copy, list));
  if (cnt == 0) {
    EXPECT_TRUE(copy.Equal(list));
  } else {
    EXPECT_FALSE(GTIDList::Subset(list, copy));
  }
}

TEST(GTIDList, Update) {
  Uuid uuid;
  std::string str = "6c27ed6d-7ee1-11e3-be39-6c626d957cff";
  EXPECT_TRUE(uuid.Parse(str));

  GTID gtid;
  gtid.domain_id = 4;
  gtid.server_id.server_id = 1;
  gtid.seq_no = 30;

  {
    GTIDList list;
    EXPECT_TRUE(list.Update(gtid));
  }

  {
    GTIDList list(GTIDList::KEY_DOMAIN_ID);
    EXPECT_TRUE(list.Update(gtid));
  }

  {
    GTIDList list(GTIDList::KEY_UUID);
    EXPECT_FALSE(list.Update(gtid));  // uuid not specified
  }

  {
    GTIDList list(GTIDList::KEY_DOMAIN_ID);
    GTID copy = gtid;
    copy.server_id.server_id = 0;
    copy.domain_id = 0;
    EXPECT_FALSE(list.Update(copy));  // server_id/domain_id not specified
  }

  {
    GTIDList list(GTIDList::KEY_DOMAIN_ID, GTIDList::MODE_MONOTONIC);
    EXPECT_TRUE(list.Update(gtid));  // mode not specified
  }

  {
    GTIDList list;
    EXPECT_TRUE(list.Update(gtid));
    EXPECT_FALSE(list.Update(gtid));
    gtid.seq_no -= 2;
    EXPECT_FALSE(list.Update(gtid));
    gtid.seq_no += 4;
    EXPECT_FALSE(list.Update(gtid));
    gtid.seq_no -= 1;
    EXPECT_TRUE(list.Update(gtid));
  }

  gtid.domain_id = 4;
  gtid.server_id.server_id = 1;
  gtid.server_id.uuid = uuid;
  gtid.seq_no = 30;

  {
    GTIDList list(GTIDList::KEY_DOMAIN_ID, GTIDList::MODE_MONOTONIC);
    RandUpdate(list, gtid);
  }

  {
    GTIDList list(GTIDList::KEY_DOMAIN_ID, GTIDList::MODE_STRICT_MONOTONIC);
    RandUpdate(list, gtid);
  }

  {
    GTIDList list(GTIDList::KEY_DOMAIN_ID, GTIDList::MODE_GAPS);
    RandUpdate(list, gtid);
  }

  {
    GTIDList list(GTIDList::KEY_UUID, GTIDList::MODE_MONOTONIC);
    RandUpdate(list, gtid);
  }

  {
    GTIDList list(GTIDList::KEY_UUID, GTIDList::MODE_STRICT_MONOTONIC);
    RandUpdate(list, gtid);
  }

  {
    GTIDList list(GTIDList::KEY_UUID, GTIDList::MODE_GAPS);
    RandUpdate(list, gtid);
  }
}

TEST(GTIDList, Assign) {
  {
    GTIDStartPosition pos;
    EXPECT_TRUE(pos.ParseMariaDBConnectState("1-2-3,2-2-3"));
    GTIDList list;
    list.Assign(pos);
    EXPECT_EQ(list.ToString(), "1-2-3,2-2-3");
  }

  {
    GTIDSet set;
    std::string str =
        "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:1-5"
        ",6c27ed6d-7ee1-11e3-be39-6c626d957cfb:3"
        ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23-29";
    EXPECT_TRUE(set.Parse(str));
    GTIDList list;
    list.Assign(set);
    EXPECT_EQ(list.ToString(),
              "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:0-0-5"
              ",6c27ed6d-7ee1-11e3-be39-6c626d957cfb:0-0-[3-3]"
              ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:0-0-[23-29]");
  }
}

TEST(GTIDList, Convert) {
  {
    GTIDStartPosition pos;
    EXPECT_TRUE(pos.ParseMariaDBConnectState("1-2-3,2-2-3"));
    GTIDList list;
    list.Assign(pos);
    EXPECT_EQ(list.ToString(), "1-2-3,2-2-3");
    EXPECT_TRUE(mysql::compat::Convert(list, &pos));
    GTIDSet set;
    EXPECT_FALSE(mysql::compat::Convert(list, &set));
  }

  {
    GTIDSet set;
    std::string str =
        "6c27ed6d-7ee1-11e3-be39-6c626d957cfa:1-5"
        ",6c27ed6d-7ee1-11e3-be39-6c626d957cfb:3"
        ",6c27ed6d-7ee1-11e3-be39-6c626d957cfc:23-29";
    EXPECT_TRUE(set.Parse(str));
    GTIDList list;
    list.Assign(set);
    EXPECT_TRUE(mysql::compat::Convert(list, &set));
    GTIDStartPosition pos;
    EXPECT_FALSE(mysql::compat::Convert(list, &pos));
  }
}

}  // namespace mysql_ripple
