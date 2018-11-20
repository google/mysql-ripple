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

#include "binlog_index.h"

#include <sys/types.h>
#include <unistd.h>

#include "gtest/gtest.h"
#include "absl/strings/escaping.h"
#include "binlog_reader.h"
#include "file.h"
#include "gtid.h"
#include "monitoring.h"

namespace mysql_ripple {

// A mock validator so that tests don't have to create actual binlog files.
struct Validator : public BinlogRecoveryHandlerInterface {
 public:
  explicit Validator(file_util::OpenResultCode return_value) {
    num_validate = 0;
    num_remove = 0;
    validate_result = return_value;
  }

  virtual ~Validator() {
  }

  file_util::OpenResultCode Validate(absl::string_view filename) override {
    num_validate++;
    return validate_result;
  }

  bool Remove(absl::string_view filename) override {
    num_remove++;
    return true;
  }

  size_t GetFileSize(absl::string_view filename) override { return 0; }

  int num_validate;
  int num_remove;
  file_util::OpenResultCode validate_result;
};

static const char *test_dir = nullptr;
const char* GetTestDir() {
  if (test_dir == nullptr) {
    if (getenv("TEST_TMPDIR")) {
      test_dir = getenv("TEST_TMPDIR");
    } else {
      test_dir = ".";
    }
  }
  return test_dir;
}

bool cleanup(const BinlogIndex& index) {
  unlink(index.GetIndexFilename().c_str());
  return true;
}

std::vector<BinlogIndex::Entry> GetEntries(const BinlogIndex& index) {
  GTIDList pos;
  std::vector<BinlogIndex::Entry> entries;
  BinlogIndex::Entry entry;
  if (index.GetEntry(pos, &entry)) {
    do {
      entries.push_back(entry);
    } while (index.GetNextEntry(entry.filename, &entry));
  }

  return entries;
}

TEST(Binlog_index, Basic) {
  monitoring::Initialize();
  auto& ff = file::FILE_Factory();
  {
    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(std::string("binlog-") + std::to_string(getpid()));
    EXPECT_TRUE(index.Create());

    BinlogIndex index2(GetTestDir(), ff);
    index2.SetBasename(index.GetBasename());
    EXPECT_FALSE(index2.Create());  // index already exist

    EXPECT_TRUE(cleanup(index));
  }

  {
    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(std::string("binlog-") + std::to_string(getpid()));
    EXPECT_TRUE(index.Create());

    Validator validator(file_util::OK);
    BinlogIndex index2(GetTestDir(), ff);
    index2.SetBasename(index.GetBasename());
    EXPECT_EQ(index2.Recover(&validator), 1);

    EXPECT_TRUE(cleanup(index));
  }

  {
    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(std::string("binlog-") + std::to_string(getpid()));
    EXPECT_TRUE(index.Create());

    EXPECT_TRUE(index.GetCurrentEntry().IsEmpty());
    GTIDList pos;
    EXPECT_TRUE(index.NewEntry(pos, FilePosition()));
    EXPECT_FALSE(index.GetCurrentEntry().IsEmpty());
    EXPECT_TRUE(index.CloseEntry(pos, FilePosition(), 0));
    EXPECT_FALSE(index.GetCurrentEntry().IsEmpty());
    EXPECT_EQ(GetEntries(index).size(), 1);
    EXPECT_TRUE(GetEntries(index)[0].start_position.IsEmpty());
    EXPECT_TRUE(GetEntries(index)[0].last_position.IsEmpty());

    EXPECT_TRUE(cleanup(index));
  }

  {
    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(std::string("binlog-") + std::to_string(getpid()));
    EXPECT_TRUE(index.Create());

    EXPECT_TRUE(index.GetCurrentEntry().IsEmpty());
    GTIDList pos;
    EXPECT_TRUE(index.NewEntry(pos, FilePosition()));
    EXPECT_FALSE(index.GetCurrentEntry().IsEmpty());
    EXPECT_TRUE(index.CloseEntry(pos, FilePosition("master-bin", 100), 0));
    EXPECT_FALSE(index.GetCurrentEntry().IsEmpty());
    EXPECT_EQ(GetEntries(index).size(), 1);
    EXPECT_TRUE(GetEntries(index)[0].start_position.IsEmpty());
    EXPECT_TRUE(GetEntries(index)[0].last_position.IsEmpty());
    EXPECT_TRUE(GetEntries(index)[0].start_master_position.IsEmpty());
    EXPECT_FALSE(GetEntries(index)[0].last_next_master_position.IsEmpty());

    EXPECT_TRUE(cleanup(index));
  }

  {
    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(std::string("binlog-") + std::to_string(getpid()));
    EXPECT_TRUE(index.Create());

    EXPECT_TRUE(index.GetCurrentEntry().IsEmpty());
    GTIDList pos;
    EXPECT_TRUE(index.NewEntry(pos, FilePosition("master-bin", 100)));
    EXPECT_FALSE(index.GetCurrentEntry().IsEmpty());
    // If start position has master-pos, then so should end.
    EXPECT_FALSE(index.CloseEntry(pos, FilePosition(), 0));

    EXPECT_TRUE(cleanup(index));
  }

  {
    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(std::string("binlog-") + std::to_string(getpid()));
    EXPECT_TRUE(index.Create());

    EXPECT_TRUE(index.GetCurrentEntry().IsEmpty());
    EXPECT_TRUE(index.Close());

    Validator validator(file_util::OK);
    EXPECT_EQ(index.Recover(&validator), 1);
    EXPECT_EQ(validator.num_validate, 0);
    EXPECT_EQ(GetEntries(index).size(), 0);
    EXPECT_TRUE(index.GetCurrentEntry().IsEmpty());
    EXPECT_TRUE(index.Close());
    EXPECT_TRUE(cleanup(index));
  }
}

TEST(Binlog_index, Recover) {
  monitoring::Initialize();
  auto& ff = file::FILE_Factory();
  struct expectation {
    std::string start_pos;
    const char *last_pos;
    file_util::OpenResultCode code;
    int recover_result;
    int no_of_entries;
  } tests[] = {
    /* tests with entries that does not have end pos */
    { "", nullptr, file_util::OK,             1, 1 },  // entry found
    { "", nullptr, file_util::NO_SUCH_FILE,   1, 0 },  // entry rolled back
    { "", nullptr, file_util::FILE_EMPTY,     1, 0 },  // entry rolled back
    { "", nullptr, file_util::INVALID_MAGIC, -1, 0 },  // error
    { "1-1-1", nullptr, file_util::OK,             1, 1 },  // entry found
    { "1-1-1", nullptr, file_util::NO_SUCH_FILE,   1, 0 },  // entry rolled back
    { "1-1-1", nullptr, file_util::FILE_EMPTY,     1, 0 },  // entry rolled back
    { "1-1-1", nullptr, file_util::INVALID_MAGIC, -1, 0 },  // error

    /* tests with entries that have empty end pos */
    { "", "", file_util::OK,             1, 1 },  // entry found
    { "", "", file_util::NO_SUCH_FILE,   1, 0 },  // entry rolled back
    { "", "", file_util::FILE_EMPTY,     1, 0 },  // entry rolled back
    { "", "", file_util::INVALID_MAGIC, -1, 0 },  // error

    /* tests with entries that have end pos */
    { "1-1-1", "1-1-1", file_util::OK,             1, 1 },  // entry found
    { "1-1-1", "1-1-1", file_util::NO_SUCH_FILE,  -1, 0 },  // error
    { "1-1-1", "1-1-1", file_util::FILE_EMPTY,    -1, 0 },  // error
    { "1-1-1", "1-1-1", file_util::INVALID_MAGIC, -1, 0 }   // error
  };

  for (const expectation& exp : tests) {
    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(std::string("binlog-") + std::to_string(getpid()));
    EXPECT_TRUE(index.Create());

    // Add one file
    GTIDList start_pos;
    if (!exp.start_pos.empty()) {
      EXPECT_TRUE(start_pos.Parse(exp.start_pos));
    }
    EXPECT_TRUE(index.NewEntry(start_pos, FilePosition("master-bin", 0)));

    if (exp.last_pos != nullptr) {
      GTIDList end_pos;
      EXPECT_TRUE(end_pos.Parse(std::string(exp.last_pos)));
      EXPECT_TRUE(index.CloseEntry(end_pos, FilePosition("master-bin", 0), 0));
    }
    EXPECT_TRUE(index.Close());

    Validator validator(exp.code);
    EXPECT_EQ(index.Recover(&validator), exp.recover_result);
    if (exp.recover_result == 1) {
      EXPECT_EQ(GetEntries(index).size(), exp.no_of_entries);
    }
    EXPECT_TRUE(index.Close());
    EXPECT_TRUE(cleanup(index));
  }
}

TEST(Binlog_index, Parse) {
  struct test {
    std::string line;
    bool ok;
  } tests[] = {
      {
          // Actual corrupt index entry from b/72149615.
          "filename=binlog.000054"
          " start_pos='0-1047290372-1913025556'"
          " start_master_pos=replication_log.000119:13400110"
          "filename=binlog.000055"
          " start_pos='0-1047290372-1913025556'"
          " start_master_pos=replication_log.000119:13400110"
          " end_pos='0-1047290372-1913322986'"
          " last_next_master_pos=replication_log.000120:321203488\n",
          false,
      },
      {
          "filename=binlog.000054 "
          "start_pos='0-1047290372-1913025556' "
          "start_master_pos=replication_log.000119:13400110",
          true,
      },
      {
          "filename=binlog.000056 "
          "start_pos='0-1047290372-1913322986' "
          "start_master_pos=replication_log.000120:321203488 "
          "end_pos='0-1047290372-1913693546' "
          "last_next_master_pos=replication_log.000122:304509981\n",
          true,
      },
      {
          // Actual valid index entry from b/78013962.
          "filename=binlog.000355 "
          "start_pos='0-965028356-2285830326' "
          "start_master_pos=replication_log.001031:506733481 "
          "end_pos='0-965028356-2286012860' "
          "last_next_master_pos=replication_log.001033:231527322 "
          "purged=1\n",
          true,
      },
  };
  for (const test& t : tests) {
    BinlogIndex::Entry entry;
    EXPECT_EQ(entry.Parse(t.line), t.ok)
        << "where t.line = \"" << absl::CEscape(t.line) << "\"";
  }
}

TEST(Binlog_index, RecoverError) {
  monitoring::Initialize();
  auto& ff = file::FILE_Factory();
  // This test creates "broken" index files that does not pass Recover

  struct line {
    const char *start_pos;
    const char *last_pos;
    const char *start_master_pos;
    const char *last_next_master_pos;
  };

  struct test {
    std::vector<line> lines;
    file_util::OpenResultCode code;
  } tests[] = {
    { // this should fail since first line has last_pos
      // then next line can't have empty start_pos
      {
        { "", "1-1-1", nullptr, nullptr },
        { "", "1-1-1", nullptr, nullptr }
      },
      file_util::OK
    },
    { // this should fail since only last line may have non-empty last if start
      // is non empty
      {
        { "1-1-1", "", nullptr, nullptr },
        { "1-1-1", "", nullptr, nullptr }
      },
      file_util::OK
    },
    {
      {
        { "", "1-1-1", "master-bin:1", nullptr },
        { "1-1-1", "1-1-1", nullptr, nullptr }
      },
      file_util::OK
    },
    {
      {
        { "", "1-1-1", "master:1", "master:2" },
        { "1-1-1", "1-1-1", "master:2", nullptr }
      },
      file_util::OK
    },
    {
      {
        { "", "1-1-1", "master:1", "master:2" },
        { "1-1-1", "1-1-1", "master:2", "master:2" }
      },
      file_util::NO_SUCH_FILE
    }
  };

  for (const test& t : tests) {
    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(std::string("binlog-") + std::to_string(getpid()));
    EXPECT_TRUE(index.Create());
    EXPECT_TRUE(index.Close());

    FILE *file = fopen(index.GetIndexFilename().c_str(), "a+");
    for (const line& l : t.lines) {
      {
        BinlogIndex::Entry entry;
        if (l.start_pos != nullptr) {
          EXPECT_TRUE(entry.start_position.Parse(l.start_pos));
        }
        if (l.start_master_pos != nullptr) {
          EXPECT_TRUE(entry.start_master_position.Parse(l.start_master_pos));
        }

        fprintf(file, "%s ", entry.FormatHead().c_str());
      }
      {
        BinlogIndex::Entry entry;
        if (l.last_pos != nullptr) {
          EXPECT_TRUE(entry.last_position.Parse(l.last_pos));
        }
        if (l.last_next_master_pos != nullptr) {
          EXPECT_TRUE(entry.last_next_master_position.Parse(
              l.last_next_master_pos));
        }
        fprintf(file, "%s", entry.FormatTail().c_str());
      }
    }
    fflush(file);
    fclose(file);

    Validator validator(t.code);
    EXPECT_EQ(index.Recover(&validator), -1);

    EXPECT_TRUE(cleanup(index));
  }
}

bool Create(BinlogIndex *index,
            const std::vector<std::pair<std::string, const char*>>& entries) {
  index->SetBasename(std::string("binlog-") + std::to_string(getpid()));
  if (!index->Create())
    return false;

  for (const auto& entry : entries) {
    GTIDList start_pos;
    if (!entry.first.empty()) {
      if (!start_pos.Parse(entry.first)) return false;
    }
    if (!index->NewEntry(start_pos, FilePosition("master-bin", 0)))
      return false;

    if (entry.second != nullptr) {
      GTIDList last_pos;
      if (strlen(entry.second)) {
        if (!last_pos.Parse(entry.second)) {
          return false;
        }
      }
      if (!index->CloseEntry(last_pos, FilePosition("master.bin", 0), 0))
        return false;
    }
  }
  return true;
}

TEST(Binlog_index, GetNext) {
  monitoring::Initialize();
  auto& ff = file::FILE_Factory();
  {
    std::string basename = std::string("binlog-") + std::to_string(getpid());
    std::string name = GetTestDir() + std::string("/") + basename + ".index";

    BinlogIndex index(GetTestDir(), ff);
    index.SetBasename(basename.c_str());

    GTIDList pos;
    BinlogIndex::Entry entry;

    // this should lead to file not found.
    EXPECT_FALSE(index.GetEntry(pos, &entry));
    EXPECT_FALSE(index.GetNextEntry(entry.filename, &entry));

    // this should lead to empty file
    FILE *file = fopen(name.c_str(), "w");
    EXPECT_FALSE(index.GetEntry(pos, &entry));
    EXPECT_FALSE(index.GetNextEntry(entry.filename, &entry));

    // this should lead to invalid magic.
    fprintf(file, "this is only junk");
    fflush(file);
    fclose(file);
    EXPECT_FALSE(index.GetEntry(pos, &entry));
    EXPECT_FALSE(index.GetNextEntry(entry.filename, &entry));

    unlink(name.c_str());
  }

  {
    std::vector<std::pair<std::string, const char*>> entries = {
      { "", "" },
      { "", "1-1-1" },
      { "1-1-1", "1-1-2" }
    };
    BinlogIndex index(GetTestDir(), ff);
    EXPECT_TRUE(Create(&index, entries));

    EXPECT_EQ(GetEntries(index).size(), 3);
    EXPECT_EQ(GetEntries(index)[0].filename, index.GetBasename()+".000000");
    EXPECT_EQ(GetEntries(index)[1].filename, index.GetBasename()+".000001");
    EXPECT_EQ(GetEntries(index)[2].filename, index.GetBasename()+".000002");

    GTIDList pos;
    EXPECT_TRUE(pos.Parse("1-1-2"));
    EXPECT_TRUE(index.NewEntry(pos, FilePosition("master-bin:0")));
    EXPECT_EQ(GetEntries(index).size(), 4);
    EXPECT_EQ(GetEntries(index)[3].filename, index.GetBasename()+".000003");

    // last file is not closed.
    EXPECT_FALSE(index.NewEntry(pos, FilePosition("master-bin:0")));

    BinlogIndex::Entry entry;
    pos.Reset();
    EXPECT_TRUE(index.GetEntry(pos, &entry));
    EXPECT_EQ(entry.filename, index.GetBasename()+".000000");

    pos.Reset();
    EXPECT_TRUE(pos.Parse("1-1-2"));
    EXPECT_EQ(pos.ToString(), "1-1-2");
    EXPECT_TRUE(index.GetEntry(pos, &entry));
    EXPECT_EQ(entry.filename, index.GetBasename()+".000002");

    // For code coverage...
    printf("index.IsOpen(): %u\n", index.IsOpen());
    printf("entry.ToString().c_str(): %s\n", entry.ToString().c_str());

    pos.Reset();
    EXPECT_TRUE(pos.Parse("1-1-3"));
    EXPECT_TRUE(index.GetEntry(pos, &entry));
    EXPECT_EQ(entry.filename, index.GetBasename()+".000003");

    // For code coverage...
    printf("entry.ToString().c_str(): %s\n", entry.ToString().c_str());
    printf("index.IsOpen(): %u\n", index.IsOpen());

    EXPECT_TRUE(index.CloseEntry(pos, FilePosition("master-bin:0"), 0));
    EXPECT_EQ(GetEntries(index).size(), 4);
    EXPECT_EQ(GetEntries(index)[3].filename, index.GetBasename()+".000003");

    EXPECT_TRUE(cleanup(index));
  }
}

}  // namespace mysql_ripple
