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

#include "file_position.h"

#include "gtest/gtest.h"

namespace mysql_ripple {

TEST(FilePosition, Parse) {
  FilePosition f1;
  EXPECT_TRUE(f1.Parse("kalle:0"));
  EXPECT_TRUE(f1.Parse("'kalle:0'"));
  EXPECT_TRUE(f1.Parse("kalle123:0"));
  EXPECT_TRUE(f1.Parse("'kalle123:0'"));
  EXPECT_FALSE(f1.Parse("kalle:0'"));
  EXPECT_FALSE(f1.Parse("'kalle:0"));
  EXPECT_FALSE(f1.Parse("kalle:0garbage"));
  EXPECT_FALSE(f1.Parse("'kalle:0'garbage"));
  EXPECT_FALSE(f1.Parse("'kalle:0garbage'"));
  EXPECT_FALSE(f1.Parse("kalle:x0"));
  EXPECT_FALSE(f1.Parse("kalle:"));
  EXPECT_FALSE(f1.Parse(":0"));
  EXPECT_FALSE(f1.Parse(":"));
  EXPECT_FALSE(f1.Parse("kalle"));
  EXPECT_FALSE(f1.Parse(""));
  EXPECT_FALSE(f1.Parse("''"));
  EXPECT_FALSE(f1.Parse("'"));
}

}  // namespace mysql_ripple
