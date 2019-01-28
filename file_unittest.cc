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

#include "file.h"
#include "gtest/gtest.h"

namespace mysql_ripple {

namespace file {

static const char *test_dir = nullptr;
const std::string GetTestDir() {
  if (test_dir == nullptr) {
    if (getenv("TEST_TMPDIR")) {
      test_dir = getenv("TEST_TMPDIR");
    } else {
      test_dir = ".";
    }
  }
  return std::string(test_dir);
}

void TestBasic(const Factory &factory, absl::string_view filename) {
  AppendOnlyFile *ofile = nullptr;
  InputFile *ifile = nullptr;
  std::string data("a string");
  int64_t offs;
  int64_t size;
  Buffer buf;

  ASSERT_TRUE(factory.Create(&ofile, filename, "a"));
  ASSERT_TRUE(factory.Open(&ifile, filename, "r"));
  EXPECT_TRUE(ofile->Tell(&offs));
  EXPECT_EQ(offs, 0);
  EXPECT_TRUE(ifile->Tell(&offs));
  EXPECT_EQ(offs, 0);
  EXPECT_TRUE(ofile->Write(data));
  EXPECT_TRUE(ofile->Sync());
  EXPECT_TRUE(ofile->Tell(&offs));
  EXPECT_EQ(offs, data.size());
  EXPECT_TRUE(ifile->Read(buf, data.size()));
  EXPECT_EQ(buf.size(), data.size());
  buf.clear();
  EXPECT_TRUE(ifile->Seek(0));
  EXPECT_TRUE(ifile->Read(buf, data.size()));
  EXPECT_EQ(buf.size(), data.size());
  EXPECT_TRUE(factory.Size(filename, &size));
  EXPECT_EQ(size, data.size());
  EXPECT_EQ(memcmp(data.data(), buf.data(), buf.size()), 0);
  EXPECT_TRUE(ofile->Truncate(0));
  EXPECT_TRUE(factory.Size(filename, &size));
  EXPECT_EQ(size, 0);
  EXPECT_TRUE(ifile->Close());
  EXPECT_TRUE(ofile->Close());
  EXPECT_TRUE(factory.Finalize(filename));
  EXPECT_TRUE(factory.Archive(filename));
  EXPECT_TRUE(factory.Delete(filename));
}

TEST(FILE, Basic) {
  TestBasic(file::FILE_Factory(), GetTestDir() + "/file.test");
}

}  // namespace file

}  // namespace mysql_ripple
