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

#ifndef MYSQL_RIPPLE_PLUGIN_H
#define MYSQL_RIPPLE_PLUGIN_H

namespace mysql_ripple {

namespace plugin {

struct Plugin {
  bool (* Init)();
};

bool InitPlugins();

}  // namespace plugin

}  // namespace mysql_ripple

#define DECLARE_PLUGIN(x) \
static bool plugin_ ## x ## _Init(); \
Plugin plugin_ ## x = { &plugin_ ## x ## _Init }; \
static bool plugin_ ## x ## _Init()

#endif  // MYSQL_RIPPLE_PLUGIN_H
