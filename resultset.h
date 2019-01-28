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

#ifndef MYSQL_RIPPLE_RESULTSET_H
#define MYSQL_RIPPLE_RESULTSET_H

#include <vector>

#include "mysql_constants.h"

namespace mysql_ripple {

namespace mysql {

class SlaveSession;  // forward decl to avoid circular includes.

}  // namespace mysql

namespace resultset {

struct ColumnDefinition {
  const char *schema;
  const char *table;
  const char *name;
  mysql_ripple::constants::DataType datatype;
  int max_length;
};

struct ColumnData {
  const char *data;
};

struct Resultset;  // forward decl.

/**
 * A context used for evalutaing queries and producing results
 */
struct QueryContext {
  const char *query;                                 // the SQL query
  mysql_ripple::mysql::SlaveSession* slave_session;  // the session
  int row_no;                                        // current row no
  Resultset *resultset;                              // the Resultset
  std::vector<std::string> storage;                  // tmp storage
};

typedef int (* RowFunction)(QueryContext *ctx);

// The NullRowFunction, means that result set is static/fixed.
const RowFunction NullRowFunction = nullptr;

struct Resultset {
  std::vector<ColumnDefinition> column_definition;
  RowFunction rowFunction;  // function producing result set
  std::vector<std::vector<ColumnData> > rows;

  bool IsFixed() const {
    return rowFunction == nullptr;
  }
};

}  // namespace resultset

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_RESULTSET_H
