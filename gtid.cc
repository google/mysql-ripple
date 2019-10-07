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

#include <functional>

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "byte_order.h"
#include "logging.h"

namespace mysql_ripple {

void Uuid::ConstructFromServerId(int server_id) {
  bytes_[6] = 13 << 4; /* version = 13 (not defined in RFC) */
  bytes_[8] = 8 << 4;  /* variant */
  /* store server_id as last 4 bytes */
  byte_order::store4(bytes_ + 12, server_id);
}

GTID::GTID() : seq_no(0), domain_id(0) {
}

std::string GTID::ToString() const {
  return ToMariaDBString();
}

std::string GTID::ToMariaDBString() const {
  return
      std::to_string(domain_id) + "-" +
      std::to_string(server_id.server_id) + "-" +
      std::to_string(seq_no);
}

bool GTID::Parse(absl::string_view s) {
  Reset();
  if (s.size() > Uuid::TEXT_LENGTH && s[Uuid::TEXT_LENGTH] == ':') {
    server_id.uuid.Parse(s.substr(0, Uuid::TEXT_LENGTH));
    s = s.substr(Uuid::TEXT_LENGTH + 1);
  }
  std::vector<absl::string_view> v = absl::StrSplit(s, absl::MaxSplits('-', 2));
  return (v.size() == 3 && absl::SimpleAtoi(v[0], &domain_id) &&
          absl::SimpleAtoi(v[1], &server_id.server_id) &&
          absl::SimpleAtoi(v[2], &seq_no));
}

bool GTIDStartPosition::Update(const GTID &other) {
  if (other.IsEmpty())
    return true;

  for (GTID& gtid : gtids) {
    if (gtid.domain_id == other.domain_id) {
      if (!(other.seq_no > gtid.seq_no)) {
        LOG(WARNING) << other.ToString().c_str()
                     << " is not a valid successor to "
                     << gtid.ToString().c_str();
        return false;
      }
      gtid = other;
      return true;
    }
  }
  gtids.push_back(other);  // new domain => add
  return true;
}

std::string GTIDStartPosition::ToString() const {
  std::string tmp;
  SerializeToString(&tmp);
  return tmp;
}

// Create string in format used in slave_connect_state for MariaDB.
void GTIDStartPosition::ToMariaDBConnectState(std::string *dst) const {
  dst->clear();
  for (const GTID& gtid : gtids) {
    if (!dst->empty()) {
      (*dst) += ",";
    }
    (*dst) += gtid.ToMariaDBString();
  }
}

namespace {
absl::string_view stripQuotes(absl::string_view s) {
  const char quote = '\'';
  if (s.size() >= 2 && s.front() == quote && s.back() == quote) {
    s.remove_prefix(1);
    s.remove_suffix(1);
  }
  return s;
}
}  // namespace

// Parse string in format used in slave_connect_state for MariaDB.
// gtid ::= <domain_id> "-" <server_id> "-" <seq_no>
// str ::= <gtid> ("," <gtid>)*
bool GTIDStartPosition::ParseMariaDBConnectState(absl::string_view s) {
  Reset();
  s = stripQuotes(s);
  if (s.empty()) {
    return true;
  }
  for (auto gtid_str : absl::StrSplit(s, ',')) {
    GTID gtid;
    if (!gtid.Parse(gtid_str)) {
      return false;
    }
    if (!gtid.server_id.uuid.empty()) {
      // should not have a UUID.
      return false;
    }
    if (!Update(gtid)) {
      return false;
    }
  }
  return true;
}

// Create string in format used in binlog index.
// [ UUID : ]? domain-serverid-sequenceno
void GTIDStartPosition::SerializeToString(std::string *dst) const {
  dst->clear();
  for (const GTID& gtid : gtids) {
    if (!dst->empty()) {
      (*dst) += ",";
    }
    if (!gtid.server_id.uuid.empty()) {
      (* dst) += gtid.server_id.uuid.ToString();
      (* dst) += ':';
    }
    (*dst) += std::to_string(gtid.domain_id);
    (*dst) += '-';
    (*dst) += std::to_string(gtid.server_id.server_id);
    (*dst) += '-';
    (*dst) += std::to_string(gtid.seq_no);
  }
}

// Parse string format used in binlog index.
// [ UUID : ]? domain-serverid-sequenceno
bool GTIDStartPosition::Parse(absl::string_view s) {
  Reset();
  for (auto gtid_str : absl::StrSplit(stripQuotes(s), ',')) {
    GTID gtid;
    if (!gtid.Parse(gtid_str)) {
      return false;
    }
    if (!Update(gtid)) {
      return false;
    }
  }
  return true;
}

bool Uuid::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len != Uuid::PACK_LENGTH) {
    return false;
  }
  memcpy(buffer, bytes_, Uuid::PACK_LENGTH);
  return true;
}

bool Uuid::ParseFromBuffer(const uint8_t *buffer, int len) {
  if (len != Uuid::PACK_LENGTH) {
    return false;
  }
  memcpy(bytes_, buffer, Uuid::PACK_LENGTH);
  return true;
}

static uint8_t char2byte(char c) {
  if (c >= '0' && c <= '9')
    return c - '0';
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 10;
  if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;

  return 255;
}

bool Uuid::Parse(absl::string_view s) {
  if (s.size() != TEXT_LENGTH) {
    return false;
  }
  uint8_t *ptr = bytes_;
  for (int i = 0; i < TEXT_LENGTH;) {
    if ((i == 8) || (i == 13) || (i == 18) || (i == 23)) {
      if (s[i++] != '-') return false;
      continue;
    }
    uint8_t byte_hi = char2byte(s[i++]);
    uint8_t byte_lo = char2byte(s[i++]);
    if (byte_hi > 15 || byte_lo > 15) return false;
    *ptr++ = (byte_hi << 4) + byte_lo;
  }
  return true;
}

bool Uuid::ToString(char *buffer, int len) const {
  if (len != TEXT_LENGTH)
    return false;

  static const char byte_to_hex[]= "0123456789abcdef";
  const uint8_t *ptr = bytes_;
  for (int i = 0; i < TEXT_LENGTH;) {
    if ((i == 8) || (i == 13) || (i == 18) || (i == 23)) {
      buffer[i] = '-';
      i++;
    } else {
      buffer[i+0] = byte_to_hex[(*ptr) >> 4];
      buffer[i+1] = byte_to_hex[(*ptr) & 0xF];

      ptr++;
      i += 2;
    }
  }
  return true;
}

std::string Uuid::ToString() const {
  char buf[TEXT_LENGTH + 1];
  ToString(buf, TEXT_LENGTH);
  buf[TEXT_LENGTH] = 0;
  std::string str(buf);
  return str;
}

int GTIDSet::PackLength() const {
  int len = 8;  // number of uuids
  for (const GTIDInterval& set : gtid_intervals_) {
    len += Uuid::PACK_LENGTH;
    len += 8;  // number of intervals
    len += 16 * set.intervals.size();
  }
  return len;
}

bool GTIDSet::SerializeToBuffer(uint8_t *buffer, int len) const {
  if (len != PackLength())
    return false;

  byte_order::store8(buffer + 0, gtid_intervals_.size());
  uint8_t *ptr = buffer + 8;
  for (const GTIDInterval& set : gtid_intervals_) {
    set.uuid.SerializeToBuffer(ptr, Uuid::PACK_LENGTH);
    ptr += Uuid::PACK_LENGTH;
    byte_order::store8(ptr, set.intervals.size());
    ptr += 8;
    for (const Interval& interval : set.intervals) {
      byte_order::store8(ptr + 0, interval.start);
      byte_order::store8(ptr + 8, interval.end);
      ptr += 16;
    }
  }
  return true;
}

bool GTIDSet::ParseFromBuffer(const uint8_t *buffer, int len) {
  Clear();
  const uint8_t *end = buffer + len;
  if (len < 8)
    return false;

  uint64_t count = byte_order::load8(buffer + 0);
  buffer += 8;

  for (uint64_t i = 0; i < count; i++) {
    if (buffer + Uuid::PACK_LENGTH > end)
      return false;

    GTIDInterval set;
    set.uuid.ParseFromBuffer(buffer, Uuid::PACK_LENGTH);
    buffer += Uuid::PACK_LENGTH;

    if (buffer + 8 > end)
      return false;

    uint64_t no_of_intervals = byte_order::load8(buffer + 0);
    buffer += 8;

    for (uint64_t j = 0; j < no_of_intervals; j++) {
      Interval interval;
      if (buffer + 16 > end)
        return false;

      interval.start = byte_order::load8(buffer + 0);
      interval.end = byte_order::load8(buffer + 8);
      buffer += 16;
      set.intervals.push_back(interval);
    }

    gtid_intervals_.push_back(set);
  }

  return buffer == end;
}

std::string GTIDSet::ToString() const {
  std::string dst;
  // guess that each interval takes 15 characters
  dst.reserve(gtid_intervals_.size() * (Uuid::TEXT_LENGTH + 1 + 15));
  for (const GTIDInterval& set : gtid_intervals_) {
    if (!dst.empty())
      dst += ',';
    size_t len = dst.length();
    dst.resize(len + Uuid::TEXT_LENGTH + 1);
    char *ptr = &dst[len];
    set.uuid.ToString(ptr, Uuid::TEXT_LENGTH);
    ptr[Uuid::TEXT_LENGTH] = ':';
    for (const Interval& interval : set.intervals) {
      if (interval.end > interval.start + 1) {
        dst += std::to_string(interval.start)
            + '-'
            +  std::to_string(interval.end - 1);
      } else {
        dst += std::to_string(interval.start);
      }
    }
  }
  return dst;
}

bool GTIDSet::Parse(absl::string_view s) {
  Clear();
  for (auto interval_str : absl::StrSplit(s, ',')) {
    GTIDInterval interval;
    if (!interval.Parse(interval_str)) {
      return false;
    }
    gtid_intervals_.push_back(interval);
  }
  return true;
}

bool GTIDSet::GTIDInterval::Parse(absl::string_view s) {
  if (s.size() <= Uuid::TEXT_LENGTH || s[Uuid::TEXT_LENGTH] != ':') {
    return false;
  }
  if (!uuid.Parse(s.substr(0, Uuid::TEXT_LENGTH))) {
    return false;
  }
  s = s.substr(Uuid::TEXT_LENGTH + 1);
  for (auto interval_str : absl::StrSplit(s, ':')) {
    Interval interval;
    if (!interval.Parse(interval_str)) {
      return false;
    }
    intervals.push_back(interval);
  }
  return true;
}

bool GTIDSet::Interval::Parse(absl::string_view s) {
  std::vector<absl::string_view> v = absl::StrSplit(s, absl::MaxSplits('-', 1));
  if (!absl::SimpleAtoi(v[0], &start)) {
    return false;
  }
  end = start;
  if (v.size() > 1 && !absl::SimpleAtoi(v[1], &end)) {
    return false;
  }
  end++;
  return true;
}

void GTIDList::Assign(const GTIDStartPosition& start_pos) {
  Reset();
  key_ = KEY_DOMAIN_ID;
  mode_ = MODE_STRICT_MONOTONIC;
  for (const GTID& gtid : start_pos.gtids) {
    GTIDStream stream;
    stream.server_id = gtid.server_id;
    stream.domain_id = gtid.domain_id;
    GTIDSet::Interval interval;
    interval.start = 1;
    interval.end = gtid.seq_no + 1;
    stream.intervals.push_back(interval);
    streams_.push_back(stream);
  }
}

void GTIDList::Assign(const GTIDSet& gtidset) {
  Reset();
  key_ = KEY_UUID;
  mode_ = MODE_GAPS;
  for (const GTIDSet::GTIDInterval& gtid_interval : gtidset.gtid_intervals_) {
    GTIDStream *stream = FindStreamByUUID(gtid_interval.uuid);
    if (stream == NULL) {
      GTIDStream new_stream;
      new_stream.server_id.uuid = gtid_interval.uuid;
      new_stream.server_id.server_id = 0;
      new_stream.domain_id = 0;
      streams_.push_back(new_stream);
      stream = &streams_.back();
    }
    for (const GTIDSet::Interval& interval : gtid_interval.intervals) {
      stream->intervals.push_back(interval);
    }
  }
}

GTIDList::GTIDStream* GTIDList::FindStream(const GTID& gtid) const {
  if (gtid.IsEmpty())
    return NULL;

  switch (key_) {
    case KEY_UNSPECIFIED:
      return NULL;
    case KEY_DOMAIN_ID:
      return FindStreamByDomainId(gtid.domain_id);
    case KEY_UUID:
      return FindStreamByUUID(gtid.server_id.uuid);
  }
  return NULL;
}

GTIDList::GTIDStream* GTIDList::FindStream(const GTIDStream& key) const {
  switch (key_) {
    case KEY_UNSPECIFIED:
      return NULL;
    case KEY_DOMAIN_ID:
      return FindStreamByDomainId(key.domain_id);
    case KEY_UUID:
      return FindStreamByUUID(key.server_id.uuid);
  }
  return NULL;
}

GTIDList::GTIDStream* GTIDList::FindStreamByDomainId(uint32_t domain_id) const {
  for (const GTIDStream& stream : streams_) {
    if (stream.domain_id == domain_id) {
      return const_cast<GTIDStream*>(&stream);
    }
  }
  return NULL;
}

GTIDList::GTIDStream* GTIDList::FindStreamByUUID(const Uuid& uuid) const {
  for (const GTIDStream& stream : streams_) {
    if (uuid.Equal(stream.server_id.uuid)) {
      return const_cast<GTIDStream*>(&stream);
    }
  }
  return NULL;
}

bool GTIDList::ValidSuccessor(const GTID& gtid) const {
  const GTIDStream* stream = FindStream(gtid);
  if (stream == NULL)
    return true;

  switch (key_) {
    case KEY_UNSPECIFIED:
      return false;
      break;
    case KEY_DOMAIN_ID:
      if (gtid.server_id.server_id == 0)
        return false;
      break;
    case KEY_UUID:
      if (gtid.server_id.uuid.empty())
        return false;
      break;
  }

  switch (mode_) {
    case MODE_UNSPECIFIED:
      return false;
    case MODE_MONOTONIC:
      return gtid.seq_no >= stream->intervals[0].end;
    case MODE_STRICT_MONOTONIC:
      return gtid.seq_no == stream->intervals[0].end;
    case MODE_GAPS:
      return !Contained(gtid);
  }
}

bool GTIDList::Contained(const GTID& gtid) const {
  const GTIDStream* stream = FindStream(gtid);
  if (stream == NULL)
    return false;

  for (const GTIDSet::Interval& interval : stream->intervals) {
    if (gtid.seq_no >= interval.start && gtid.seq_no < interval.end)
      return true;
  }

  return false;
}

void GTIDList::GuessModeFromGTID(const GTID& gtid) {
  if (key_ == KEY_UNSPECIFIED) {
    if (gtid.domain_id != 0 || gtid.server_id.server_id != 0) {
      key_ = KEY_DOMAIN_ID;
    } else if (!gtid.server_id.uuid.empty()) {
      key_ = KEY_UUID;
    }
  }

  if (mode_ == MODE_UNSPECIFIED) {
    /* base mode guess on key_ */
    switch (key_) {
      case KEY_UNSPECIFIED:
        break;
      case KEY_DOMAIN_ID:
        mode_ = MODE_STRICT_MONOTONIC;
        break;
      case KEY_UUID:
        mode_ = MODE_GAPS;
    }
  }
}

bool GTIDList::Update(const GTID& gtid) {
  if (streams_.empty()) {
    GuessModeFromGTID(gtid);
  }

  switch (key_) {
    case KEY_UNSPECIFIED:
      return false;
    case KEY_DOMAIN_ID:
      if (gtid.server_id.server_id == 0)
        return false;
      break;
    case KEY_UUID:
      if (gtid.server_id.uuid.empty())
        return false;
      break;
  }

  GTIDStream* stream = FindStream(gtid);
  if (stream == NULL) {
    GTIDStream new_stream;
    new_stream.server_id = gtid.server_id;
    new_stream.domain_id = gtid.domain_id;
    GTIDSet::Interval interval;

    switch (mode_) {
      case MODE_UNSPECIFIED:
        return false;
      case MODE_MONOTONIC:
      case MODE_STRICT_MONOTONIC:
        interval.start = 1;
        interval.end = gtid.seq_no + 1;
        break;
      case MODE_GAPS:
        interval.start = gtid.seq_no;
        interval.end = gtid.seq_no + 1;
        break;
    }
    new_stream.intervals.push_back(interval);
    streams_.push_back(new_stream);
    return true;
  }

  switch (mode_) {
    case MODE_UNSPECIFIED:
      return false;
    case MODE_MONOTONIC:
      if (gtid.seq_no >= stream->intervals[0].end) {
        stream->server_id = gtid.server_id;
        stream->intervals[0].end = gtid.seq_no + 1;
        return true;
      }
      break;
    case MODE_STRICT_MONOTONIC:
      if (gtid.seq_no == stream->intervals[0].end) {
        stream->server_id = gtid.server_id;
        stream->intervals[0].end = gtid.seq_no + 1;
        return true;
      }
      break;
    case MODE_GAPS:
      // need to search...
      for (size_t i = 0; i < stream->intervals.size(); i++) {
        if (gtid.seq_no >= stream->intervals[i].start &&
            gtid.seq_no < stream->intervals[i].end) {
          // already contained!
          return false;
        }

        if (stream->intervals[i].end == gtid.seq_no) {
          // extend interval
          stream->server_id = gtid.server_id;
          stream->intervals[i].end = gtid.seq_no + 1;
          CheckMerge(stream, i);
          return true;
        } else if (gtid.seq_no + 1 == stream->intervals[i].start) {
          // extend interval
          stream->server_id = gtid.server_id;
          stream->intervals[i].start = gtid.seq_no;
          CheckMerge(stream, i - 1);
          return true;
        } else if (gtid.seq_no < stream->intervals[i].start) {
          // we need to insert before i
          break;
        }
      }
      GTIDSet::Interval new_interval;
      new_interval.start = gtid.seq_no;
      new_interval.end = gtid.seq_no + 1;
      for (auto it = stream->intervals.begin(); it != stream->intervals.end();
           ++it) {
        if (gtid.seq_no < (* it).start) {
          stream->intervals.insert(it, new_interval);
          return true;
        }
      }
      stream->server_id = gtid.server_id;
      stream->intervals.push_back(new_interval);
      return true;
      break;
  }
  return false;
}

void GTIDList::CheckMerge(GTIDStream *stream, int i) {
  if (i < 0)
    return;
  if (i + 1 >= stream->intervals.size())
    return;
  if (stream->intervals[i].end != stream->intervals[i+1].start)
    return;
  stream->intervals[i].end = stream->intervals[i+1].end;
  stream->intervals.erase(stream->intervals.begin() + i + 1,
                          stream->intervals.begin() + i + 1 + 1);
}

// Create string in format used in binlog index.
// [ UUID : ]? domain-serverid-sequenceno
void GTIDList::SerializeToString(std::string *dst) const {
  dst->clear();
  for (const GTIDStream& stream : streams_) {
    if (!dst->empty()) {
      (*dst) += ",";
    }
    if (!stream.server_id.uuid.empty()) {
      (* dst) += stream.server_id.uuid.ToString();
      (* dst) += ':';
    }
    (*dst) += std::to_string(stream.domain_id);
    (*dst) += '-';
    (*dst) += std::to_string(stream.server_id.server_id);
    (*dst) += '-';
    if (stream.intervals.size() == 1 && stream.intervals[0].start == 1) {
      (*dst) += std::to_string(stream.intervals[0].end - 1);
    } else {
      for (const GTIDSet::Interval& interval : stream.intervals) {
        (*dst) += '[';
        (*dst) += std::to_string(interval.start);
        (*dst) += '-';
        (*dst) += std::to_string(interval.end - 1);
        (*dst) += ']';
      }
    }
  }
}

bool GTIDList::GTIDStream::Parse(absl::string_view s) {
  if (s.size() > Uuid::TEXT_LENGTH && s[Uuid::TEXT_LENGTH] == ':') {
    server_id.uuid.Parse(s.substr(0, Uuid::TEXT_LENGTH));
    s = s.substr(Uuid::TEXT_LENGTH + 1);
  }
  std::vector<absl::string_view> v = absl::StrSplit(s, absl::MaxSplits('-', 2));
  if (v.size() != 3) {
    return false;
  }
  if (!absl::SimpleAtoi(v[0], &domain_id)) {
    return false;
  }
  if (!absl::SimpleAtoi(v[1], &server_id.server_id)) {
    return false;
  }
  s = v[2];
  if (!s.empty() && s.front() == '[' && s.back() == ']') {
    s.remove_prefix(1);
    s.remove_suffix(1);
    for (auto interval_str : absl::StrSplit(s, "][")) {
      GTIDSet::Interval interval;
      if (!interval.Parse(interval_str)) {
        return false;
      }
      intervals.push_back(interval);
    }
    return true;
  }
  uint64_t seq_no;
  if (!absl::SimpleAtoi(s, &seq_no)) {
    return false;
  }
  GTIDSet::Interval interval;
  interval.start = 1;
  interval.end = seq_no + 1;
  intervals.push_back(interval);
  return true;
}

//   str = "6c27ed6d-7ee1-11e3-be39-6c626d957cfc:1-1-,2-2-2";
// Parse string format used in binlog index.
// [ UUID : ]? domain-serverid-sequenceno
bool GTIDList::Parse(absl::string_view s) {
  Reset();
  StreamKey key_guess = KEY_UNSPECIFIED;
  ListMode mode_guess = MODE_UNSPECIFIED;
  s = stripQuotes(s);
  if (s.empty()) {
    key_ = key_guess;
    mode_ = mode_guess;

    return Validate();
  }
  for (absl::string_view s : absl::StrSplit(s, ',')) {
    GTIDStream stream;
    if (!stream.Parse(s)) {
      return false;
    }
    if (stream.domain_id != 0 || stream.server_id.server_id != 0) {
      key_guess = KEY_DOMAIN_ID;
      mode_guess = MODE_STRICT_MONOTONIC;
      if (stream.intervals.size() != 1) {
        LOG(WARNING) << "GTIDList for MariaDB is incorrect, "
                     << "contains several invervals: " << s;
        return false;
      }
    } else if (!stream.server_id.uuid.empty()) {
      key_guess = KEY_UUID;
      mode_guess = MODE_GAPS;
    }

    streams_.push_back(stream);
  }

  key_ = key_guess;
  mode_ = mode_guess;

  return Validate();
}

bool GTIDList::Validate() const {
  switch (key_) {
    case KEY_UNSPECIFIED:
      if (!streams_.empty())
        return false;
      break;
    case KEY_DOMAIN_ID:
      for (auto it1 = streams_.begin(); it1 != streams_.end(); ++it1) {
        for (auto it2 = it1 + 1; it2 != streams_.end(); ++it2) {
          if ((*it1).domain_id == (*it2).domain_id) {
            // duplicate domain_id
            return false;
          }
        }
      }
      break;
    case KEY_UUID:
      for (auto it1 = streams_.begin(); it1 != streams_.end(); ++it1) {
        for (auto it2 = it1 + 1; it2 != streams_.end(); ++it2) {
          if ((*it1).server_id.uuid.Equal((*it2).server_id.uuid)) {
            // duplicate domain_id
            return false;
          }
        }
      }
      break;
  }

  switch (mode_) {
    case MODE_UNSPECIFIED:
      return streams_.empty();
    case MODE_MONOTONIC:
    case MODE_STRICT_MONOTONIC:
      for (auto it = streams_.begin(); it != streams_.end(); ++it) {
        if ((*it).intervals.size() != 1 || (*it).intervals[0].start != 1) {
          return false;
        }
      }
      break;
    case MODE_GAPS:
      break;
  }

  return true;
}

bool GTIDList::Equal(const GTIDList& B) const {
  return ToString().compare(B.ToString()) == 0;
}

// Are all gtids represented in A contained in B ?
bool GTIDList::Subset(const GTIDList& A, const GTIDList& B) {
  using I = const GTIDSet::Interval;
  for (auto& stream_A : A.streams_) {
    auto stream_B = B.FindStream(stream_A);
    if (stream_B == nullptr) {
      return false;
    }
    auto a = stream_A.intervals;
    auto b = stream_B->intervals;
    bool ok = std::all_of(a.begin(), a.end(), [b](I& a) {
      return std::any_of(b.begin(), b.end(), [a](I& b) {
        return a.start >= b.start && a.end <= b.end;
      });
    });
    if (!ok) {
      return false;
    }
  }
  return true;
}
}  // namespace mysql_ripple
