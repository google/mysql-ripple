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

#ifndef MYSQL_RIPPLE_MYSQL_BYTE_ORDER_H
#define MYSQL_RIPPLE_MYSQL_BYTE_ORDER_H

#include <cassert>
#include <cstdint>

namespace mysql_ripple {

namespace byte_order {

static inline void store1(void *ptr, uint64_t val) {
  assert(val < (uint64_t(1) << 8));
  uint8_t *tmp = reinterpret_cast<uint8_t*>(ptr);
  tmp[0] = (val >> 0) & 0xFF;
}

static inline void store2(void *ptr, uint64_t val) {
  assert(val < (uint64_t(1) << 16));
  uint8_t *tmp = reinterpret_cast<uint8_t*>(ptr);
  tmp[0] = (val >> 0) & 0xFF;
  tmp[1] = (val >> 8) & 0xFF;
}

static inline void store3(void *ptr, uint64_t val) {
  assert(val < (uint64_t(1) << 24));
  uint8_t *tmp = reinterpret_cast<uint8_t*>(ptr);
  tmp[0] = (val >> 0) & 0xFF;
  tmp[1] = (val >> 8) & 0xFF;
  tmp[2] = (val >> 16) & 0xFF;
}

static inline void store4(void *ptr, uint64_t val) {
  assert(val < (uint64_t(1) << 32));
  uint8_t *tmp = reinterpret_cast<uint8_t*>(ptr);
  tmp[0] = (val >> 0) & 0xFF;
  tmp[1] = (val >> 8) & 0xFF;
  tmp[2] = (val >> 16) & 0xFF;
  tmp[3] = (val >> 24) & 0xFF;
}

static inline void store8(void *ptr, uint64_t val) {
  uint8_t *tmp = reinterpret_cast<uint8_t*>(ptr);
  tmp[0] = (val >> 0) & 0xFF;
  tmp[1] = (val >> 8) & 0xFF;
  tmp[2] = (val >> 16) & 0xFF;
  tmp[3] = (val >> 24) & 0xFF;
  tmp[4] = (val >> 32) & 0xFF;
  tmp[5] = (val >> 40) & 0xFF;
  tmp[6] = (val >> 48) & 0xFF;
  tmp[7] = (val >> 56) & 0xFF;
}

static inline uint8_t load1(const void *ptr) {
  const uint8_t *tmp = reinterpret_cast<const uint8_t*>(ptr);
  return tmp[0];
}

static inline uint16_t load2(const void *ptr) {
  const uint8_t *tmp = reinterpret_cast<const uint8_t*>(ptr);
  return
      (((uint16_t)tmp[0]) << 0) +
      (((uint16_t)tmp[1]) << 8);
}

static inline uint32_t load3(const void *ptr) {
  const uint8_t *tmp = reinterpret_cast<const uint8_t*>(ptr);
  return
      (((uint32_t)tmp[0]) << 0) +
      (((uint32_t)tmp[1]) << 8) +
      (((uint32_t)tmp[2]) << 16);
}

static inline uint32_t load4(const void *ptr) {
  const uint8_t *tmp = reinterpret_cast<const uint8_t*>(ptr);
  return
      (((uint32_t)tmp[0]) << 0) +
      (((uint32_t)tmp[1]) << 8) +
      (((uint32_t)tmp[2]) << 16) +
      (((uint32_t)tmp[3]) << 24);
}

static inline uint64_t load8(const void *ptr) {
  const uint8_t *tmp = reinterpret_cast<const uint8_t*>(ptr);
  return
      (((uint64_t)tmp[0]) << 0) +
      (((uint64_t)tmp[1]) << 8) +
      (((uint64_t)tmp[2]) << 16) +
      (((uint64_t)tmp[3]) << 24) +
      (((uint64_t)tmp[4]) << 32) +
      (((uint64_t)tmp[5]) << 40) +
      (((uint64_t)tmp[6]) << 48) +
      (((uint64_t)tmp[7]) << 56);
}

}  // namespace byte_order

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_MYSQL_BYTE_ORDER_H
