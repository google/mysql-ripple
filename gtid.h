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

#ifndef MYSQL_RIPPLE_GTID_H
#define MYSQL_RIPPLE_GTID_H

#include <map>
#include <string>
#include <cstdint>
#include <vector>

#include "absl/strings/string_view.h"
#include "mysql_compat.h"

namespace mysql_ripple {

// Uuid with MySQL Parse/Serialize
struct Uuid {
  static const int PACK_LENGTH = 16;
  static const int TEXT_LENGTH = 36;

  Uuid() {
    clear();
  }

  void clear() {
    memset(bytes_, 0, sizeof(bytes_));
  }

  bool empty() const {
    uint8_t zero[sizeof(bytes_)] = { 0 };
    return memcmp(bytes_, zero, sizeof(zero)) == 0;
  }

  // Convert to/from string representation.
  std::string ToString() const;
  bool ToString(char *buffer, int len) const;
  bool Parse(absl::string_view);

  // Convert to/from binary representation used
  // by mysqld.
  bool ParseFromBuffer(const uint8_t *buffer, int len);
  bool SerializeToBuffer(uint8_t *buffer, int len) const;

  bool Equal(const Uuid& other) const {
    return memcmp(bytes_, other.bytes_, sizeof(bytes_)) == 0;
  }

  // Construct a fake uuid from server_id.
  void ConstructFromServerId(int server_id);

 private:
  uint8_t bytes_[PACK_LENGTH];
};

// This class represents an server identifier.
// For MariaDB server_id is used,
// for MySQL uuid is used.
struct ServerId {
  ServerId() : server_id(0) {}
  void reset() { server_id = 0; uuid.clear(); }
  void assign(uint64_t id) {
    if (id != server_id) {
      server_id = id;
      uuid.clear();
    }
  }

  bool empty() const {
    return server_id == 0 && uuid.empty();
  }

  bool equal(const ServerId& other) const {
    return server_id == other.server_id &&
        uuid.Equal(other.uuid);
  }

  Uuid uuid;
  uint64_t server_id;
};

// This class represents a GTID.
struct GTID {
  GTID();

  ServerId server_id;
  uint64_t seq_no;
  uint32_t domain_id;  // Only used by MariaDB

  void Reset() {
    server_id.reset();
    seq_no = 0;
    domain_id = 0;
  }

  GTID& set_server_id(uint64_t val) { server_id.assign(val); return *this;}
  GTID& set_domain_id(uint32_t val) { domain_id = val; return *this;}
  GTID& set_sequence_no(uint64_t val) { seq_no = val; return *this;}

  bool IsEmpty() const {
    return (server_id.empty() && domain_id == 0 && seq_no == 0);
  }

  bool equal(const GTID& other) const {
    return
        (other.domain_id == domain_id) &&
        (other.seq_no == seq_no) &&
        (other.server_id.equal(server_id));
  }

  std::string ToString() const;
  std::string ToMariaDBString() const;
  bool Parse(absl::string_view);
};


// This class represents a start position.
// This means last GTID executed per domain,
// i.e last GTID not needed per domain.
// Or in MySQL terminology executed_gtid_set = { -inf - GTIDStartPosition }
struct GTIDStartPosition {
  std::vector<GTID> gtids;

  bool IsEmpty() const {
    return gtids.empty();
  }

  void Reset() {
    gtids.clear();
  }

  bool Equal(const GTIDStartPosition& other) const {
    if (gtids.size() != other.gtids.size())
      return false;

    for (const GTID& g1 : gtids) {
      bool found = false;
      for (const GTID& g2 : other.gtids) {
        if (g1.equal(g2)) {
          found = true;
          break;
        }
      }
      if (!found)
        return false;
    }
    return true;
  }

  // Check if g2 is valid to put in binlog after p1
  static bool ValidSuccessor(const GTIDStartPosition& p1,
                             const GTID& g2) {
    for (const GTID& g1 : p1.gtids) {
      if (g1.domain_id == g2.domain_id)
        return g2.seq_no > g1.seq_no;
    }
    return true;
  }

  // Check if p2 is strictly after p1
  // I.e all GTIDs in p2 are ValidSuccessor in p1.
  static bool IsAfter(const GTIDStartPosition& p1,
                      const GTIDStartPosition& p2) {
    for (const GTID& g2 : p2.gtids) {
      if (!ValidSuccessor(p1, g2))
        return false;
    }
    return true;
  }

  bool ValidSuccessor(const GTID& gtid) const {
    return ValidSuccessor(*this, gtid);
  }

  // Check if this is contained in start-end interval.
  // Note that start position not included.
  bool IsContained(const GTIDStartPosition& start,
                   const GTIDStartPosition& last) const {
    if (IsEmpty())
      return true;

    if (start.IsEmpty() && last.IsEmpty())
      return false;

    if (!last.IsEmpty()) {
      if (IsAfter(last, *this)) {
        return false;
      }
    }

    if (!start.IsEmpty()) {
      if (!IsAfter(start, *this)) {
        return false;
      }
    }

    return true;
  }

  // For logging/debugging.
  std::string ToString() const;

  // Set/Parse slave_connect_state set by MariaDB.
  void ToMariaDBConnectState(std::string *dst) const;
  bool ParseMariaDBConnectState(absl::string_view);

  // Set/Parse string format used in binlog index.
  // [ UUID : ]? domain-serverid-sequenceno
  void SerializeToString(std::string *dst) const;
  bool Parse(absl::string_view);

  bool Update(const GTID&);
};

// GTIDSet as represented by MySQL
struct GTIDSet {
  struct Interval {
    uint64_t start;  // inclusive
    uint64_t end;    // not inclusive
    bool Parse(absl::string_view s);
  };

  struct GTIDInterval {
    Uuid uuid;
    std::vector<Interval> intervals;
    bool Parse(absl::string_view s);
  };

  // Clear
  void Clear() { gtid_intervals_.clear(); }

  bool Parse(absl::string_view s);
  std::string ToString() const;

  // Parse encoded GTIDSet as written by MySQL
  bool ParseFromBuffer(const uint8_t *ptr, int len);

  // Write encoded GTIDSet in MySQL format
  int PackLength() const;
  bool SerializeToBuffer(uint8_t *buffer, int len) const;

  //
  void AddInterval(const GTIDInterval& gtid_interval) {
    gtid_intervals_.push_back(gtid_interval);
  }

  std::vector<GTIDInterval> gtid_intervals_;
};

// This is the canonical representation of executed gtids in ripple.
// It's a superset of GTIDStartPosition and GTIDSet
// The class can use either UUID or domain_id as "key".
class GTIDList {
 public:
  // What is key of a stream.
  // MariaDB: domain_id
  // MySQL: uuid
  enum StreamKey {
    KEY_UNSPECIFIED,
    KEY_DOMAIN_ID,
    KEY_UUID,
  };

  // Mode for GTIDList
  // MariaDB: MODE_MONOTONIC/MODE_STRICT_MONOTONIC
  // MySQL: MODE_GAPS
  enum ListMode {
    MODE_UNSPECIFIED,
    MODE_MONOTONIC,
    MODE_STRICT_MONOTONIC,
    MODE_GAPS,
  };

  GTIDList(StreamKey key = KEY_UNSPECIFIED, ListMode mode = MODE_UNSPECIFIED)
      : key_(key), mode_(mode)
  {}

  bool IsEmpty() const {
    return streams_.empty();
  }

  void Reset() {
    streams_.clear();
  }

  // Assign GTIDList from GTIDStartPosition or GTIDSet.
  void Assign(const GTIDStartPosition& start_pos);
  void Assign(const GTIDSet& gtidset);

  //
  bool Equal(const GTIDList&) const;

  // Is gtid part of this gtid list?
  bool Contained(const GTID& gtid) const;

  // Are all gtids represented in A contained in B ?
  static bool Subset(const GTIDList& A, const GTIDList& B);

  // Is gtid valid to include ?
  // This depends on setting for strict monotonically increasing
  bool ValidSuccessor(const GTID& gtid) const;

  // Include gtid into this gtid list.
  bool Update(const GTID& gtid);

  // Set/Parse string format used in binlog index.
  // [ UUID : ]? domain-serverid-sequenceno
  // [ UUID : ]? domain-serverid-[lo-hi]
  void SerializeToString(std::string *dst) const;
  bool Parse(absl::string_view s);

  // for debugging...
  std::string ToString() const {
    std::string tmp;
    SerializeToString(&tmp);
    return tmp;
  }

  StreamKey GetStreamKey() const {
    return key_;
  }

  ListMode GetListMode() const {
    return mode_;
  }

  std::string GetConfigString() const {
    std::string ret = "key=";
    switch (GetStreamKey()) {
      case KEY_UNSPECIFIED:
        ret += "unspecified";
        break;
      case KEY_DOMAIN_ID:
        ret += "domain_id";
        break;
      case KEY_UUID:
        ret += "uuid";
        break;
    }
    ret += ", ";
    ret += "mode=";
    switch (GetListMode()) {
      case MODE_UNSPECIFIED:
        ret += "unspecified";
        break;
      case MODE_MONOTONIC:
        ret += "monotonic";
        break;
      case MODE_STRICT_MONOTONIC:
        ret += "strict_monotonic";
        break;
      case MODE_GAPS:
        ret += "gaps";
        break;
    }
    return ret;
  }

 private:
  struct GTIDStream {
    ServerId server_id;
    uint32_t domain_id;
    std::vector<GTIDSet::Interval> intervals;
    bool Parse(absl::string_view);
  };

  StreamKey key_;
  ListMode mode_;
  std::vector<GTIDStream> streams_;

  GTIDStream* FindStream(const GTID&) const;
  GTIDStream* FindStream(const GTIDStream&) const;
  GTIDStream* FindStreamByDomainId(uint32_t domain_id) const;
  GTIDStream* FindStreamByUUID(const Uuid& uuid) const;
  void CheckMerge(GTIDStream *stream, int i);
  void GuessModeFromGTID(const GTID&);

  // Check that data is consistent wrt key_ and mode_
  bool Validate() const;

  friend bool mysql::compat::Convert(const GTIDList&, GTIDSet *);
  friend bool mysql::compat::Convert(const GTIDList&, GTIDStartPosition *);
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_GTID_H
