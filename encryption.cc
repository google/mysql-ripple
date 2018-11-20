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

#include "encryption.h"

#include <arpa/inet.h>  // htonl

#include <cstdlib>
#include <cstring>

#include "absl/strings/string_view.h"
#include "my_crypt.h"
#include "my_crypt_key_management.h"
#include "byte_order.h"
#include "flags.h"
#include "log_event.h"
#include "logging.h"
#include "monitoring.h"
#include "mysql_constants.h"

namespace mysql_ripple {

static const int kKeyLength = 16;
static const int kNonceLength = 16;
static const int kIvLength = 20;
static const int kTagLength = 16;

class ExampleKeyHandler : public KeyHandler {
 public:
  ExampleKeyHandler() {}
  virtual ~ExampleKeyHandler() {}

  KeyHandler *Copy() const override {
    return new ExampleKeyHandler();
  }

  uint32_t GetLatestKeyVersion() override {
    return time(0);
  }

  bool GetKeyBytes(uint32_t key_version, uint8_t *dst, int len) {
    // This code is copied from MariaDB to produce compatible
    // keys in debug mode
    if (len < sizeof(key_version))
      return false;
    memset(dst, 0, len);
    key_version = htonl(key_version);
    memcpy(dst, &key_version, sizeof(key_version));
    return true;
  }
};

KeyHandler* KeyHandler::GetInstance(bool example) {
  if (example)
    return new ExampleKeyHandler();
  else
    return new KeyHandler();
}

KeyHandler* KeyHandler::Copy() const {
  return new KeyHandler();
}

uint32_t KeyHandler::GetLatestKeyVersion() {
  return ::GetLatestCryptoKeyVersion();
}

bool KeyHandler::GetKeyBytes(uint32_t key_version, uint8_t *dst, int len) {
  return ::GetCryptoKey(key_version, dst, len) == 0;
}

bool KeyHandler::GetRandomBytes(uint8_t *dst, int len) {
  return ::RandomBytes(dst, len) == CRYPT_OK;
}

AesGcmBinlogEncryptor::AesGcmBinlogEncryptor(int crypt_scheme,
                                             KeyHandler *key_handler)
    : crypt_scheme_(crypt_scheme), key_handler_(key_handler) {
  key_.Append(kKeyLength);
  iv_.Append(kIvLength);
}

AesGcmBinlogEncryptor::AesGcmBinlogEncryptor(const AesGcmBinlogEncryptor& orig)
  : crypt_scheme_(orig.crypt_scheme_),
    key_handler_(orig.key_handler_->Copy()) {
  key_.Append(kKeyLength);
  iv_.Append(kIvLength);
  SetKeyAndNonce(orig.key_version_, orig.iv_.data(), kNonceLength);
}

bool AesGcmBinlogEncryptor::Init() {
  Buffer buf;
  buf.Append(kNonceLength);
  if (!key_handler_->GetRandomBytes(buf.data(), buf.size())) {
    return false;
  }
  return SetKeyAndNonce(key_handler_->GetLatestKeyVersion(),
                        buf.data(), buf.size());
}

BinlogEncryptor *AesGcmBinlogEncryptor::Copy() const {
  return new AesGcmBinlogEncryptor(*this);
}

bool AesGcmBinlogEncryptor::SetKeyAndNonce(uint32_t version,
                                           const uint8_t *nonce, int len) {
  assert(key_.size() == kKeyLength);
  assert(iv_.size() == kIvLength);

  if (len != kNonceLength) {
    return false;
  }

  key_version_ = version;
  if (!key_handler_->GetKeyBytes(version, key_.data(), key_.size())) {
    LOG(ERROR) << "Failed to GetKeyBytes()"
               << ", version: " << version
               << ", size: " << key_.size();
    monitoring::rippled_binlog_error->Increment(
      monitoring::ERROR_GET_KEY_BYTES);
    return false;
  }
  memcpy(iv_.data(), nonce, len);

  return true;
}

bool AesGcmBinlogEncryptor::Encrypt(off_t pos, const uint8_t *src, int len,
                                    Buffer *dst) {
  byte_order::store4(iv_.data() + kNonceLength, pos);
  Aes128GcmEncrypter encrypter;
  if (encrypter.Init(key_.data(), iv_.data(), iv_.size()) != CRYPT_OK) {
    LOG(ERROR) << "Failed to encrypt log event"
               << ", encrypter.Init() failed";
    monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_INIT_ENCRYPTOR);
    return false;
  }

  dst->resize(4 + len + kTagLength);

  // 1. store length.
  byte_order::store4(dst->data(), len);

  // 2. store encrypted event.
  int encrypted_len = 0;
  if (encrypter.Encrypt(src, len,
                        dst->data() + 4, &encrypted_len) != CRYPT_OK) {
    LOG(ERROR) << "Failed to encrypt log event"
               << ", encrypter.Encrypt() failed";
    monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_ENCRYPT);
    return false;
  }

  if (encrypted_len != len) {
    LOG(ERROR) << "Failed to encrypt log event"
               << ", encrypted_len: " << encrypted_len
               << ", len: " << len;
    monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_ENCRYPT);
    return false;
  }

  // 3. store tag.
  if (encrypter.GetTag(dst->data() + 4 + len, kTagLength) != CRYPT_OK) {
    LOG(ERROR) << "Failed to encrypt log event"
               << ", encrypter.GetTag() returned error";
    monitoring::rippled_binlog_error->Increment(
        monitoring::ERROR_ENCRYPT);
    return false;
  }

  return true;
}

bool AesGcmBinlogEncryptor::Decrypt(off_t pos, const uint8_t *src, int len,
                                    Buffer *dst) {
  byte_order::store4(iv_.data() + kNonceLength, pos);
  Aes128GcmDecrypter decrypter;
  if (decrypter.Init(key_.data(), iv_.data(), iv_.size()) != CRYPT_OK) {
    LOG(WARNING) << "Failed to decrypt log event"
                 << ", decrypter.Init() failed";
    return false;
  }

  // 1. read length.
  int event_len = byte_order::load4(src);
  if (len != (4 + event_len + kTagLength)) {
    LOG(WARNING) << "Failed to decrypt log event"
                 << ", len: " << len
                 << ", event_len: " << event_len
                 << ", kTagLength: " << kTagLength;
    return false;
  }

  // 2. read and set tag.
  if (decrypter.SetTag(src + 4 + event_len, kTagLength) != CRYPT_OK) {
    LOG(WARNING) << "Failed to decrypt log event"
                 << ", decrypter.SetTag() failed";
    return false;
  }

  // 3. decrypt.
  int decrypted_len = 0;
  dst->resize(event_len);
  if (decrypter.Decrypt(src + 4, event_len,
                        dst->data(), &decrypted_len) != CRYPT_OK) {
    LOG(WARNING) << "Failed to decrypt log event"
                 << ", decrypter.Decrypt() failed";
    return false;
  }

  if (decrypted_len != event_len) {
    LOG(WARNING) << "Failed to decrypt log event"
                 << ", decrypted_len: " << decrypted_len
                 << ", event_len: " << event_len;
    return false;
  }

  // 4. check tag.
  if (decrypter.CheckTag() != CRYPT_OK) {
    LOG(WARNING) << "Failed to decrypt log event"
                 << ", decrypter.CheckTag() failed";
    return false;
  }

  return true;
}

int AesGcmBinlogEncryptor::GetExtraSize() const {
  return 4 + kTagLength;
}

file_util::ReadResultCode AesGcmBinlogEncryptor::Read(file::InputFile *file,
                                                      off_t end_of_file,
                                                      Buffer *dst) {
  int64_t offset;
  file->Tell(&offset);

  if (offset + 4 > end_of_file) {
    return file_util::READ_EOF;
  }

  buffer_.clear();
  if (!file->Read(buffer_, 4)) {
    LOG(WARNING) << "Failed to read encrypted log event"
                 << ", offset: " << offset;
    return file_util::READ_ERROR;
  }

  int len = byte_order::load4(buffer_.data());
  int remaining = len + GetExtraSize() - 4;
  if (offset + len + GetExtraSize() > end_of_file) {
    LOG(WARNING) << "Failed to read encrypted log event"
                 << ", offset: " << offset
                 << ", len: " << len
                 << ", GetExtraSize(): " << GetExtraSize()
                 << ", end_of_file: " << end_of_file;
    return file_util::READ_EOF;
  }

  if (remaining != 0 && !file->Read(buffer_, remaining)) {
    LOG(WARNING) << "Failed to read encrypted log event"
                 << ", offset: " << offset << ", remaining: " << remaining;
    return file_util::READ_ERROR;
  }

  if (!Decrypt(offset, buffer_.data(), buffer_.size(), dst)) {
    LOG(WARNING) << "Failed to read encrypted log event"
                 << ", offset: " << offset
                 << ", Decrypt() failed";
    return file_util::READ_ERROR;
  }

  return file_util::READ_OK;
}

bool AesGcmBinlogEncryptor::Write(file::AppendOnlyFile *file,
                                  absl::string_view src) {
  int64_t offset;
  if (!file->Tell(&offset)) {
    LOG(WARNING) << "Failed to write encrypted log event"
                 << ", Tell() failed!";
    return false;
  }
  if (!Encrypt(offset, reinterpret_cast<const unsigned char *>(src.data()),
               src.size(), &buffer_)) {
    LOG(WARNING) << "Failed to write encrypted log event"
                 << ", offset: " << offset
                 << ", Encrypt() failed!";
    return false;
  }

  absl::string_view data(reinterpret_cast<const char *>(buffer_.data()),
                         buffer_.size());
  if (!file->Write(data)) {
    LOG(WARNING) << "Failed to write encrypted log event"
                 << ", offset: " << offset << ", length: " << buffer_.size();
    return false;
  }

  return true;
}

int AesGcmBinlogEncryptor::GetStartEncryptionEvent(const LogEventHeader& head,
                                                   Buffer *dst) const {
  StartEncryptionEvent event;
  event.crypt_scheme = crypt_scheme_;
  if (crypt_scheme_ == 255 && FLAGS_danger_danger_use_dbug_keys) {
    // store 1 even if we use fake keys (i.e 255)
    event.crypt_scheme = 1;
  } else {
    event.crypt_scheme = crypt_scheme_;
  }
  event.key_version = key_version_;
  event.nonce.assign(reinterpret_cast<const char*>(iv_.data()), kNonceLength);

  LogEventHeader header(head);
  header.type = constants::ET_START_ENCRYPTION;
  header.event_length = header.PackLength() + event.PackLength();
  header.nextpos += header.event_length;

  uint8_t *ptr = dst->Append(header.event_length);
  if (!header.SerializeToBuffer(ptr, header.event_length))
    return -1;
  if (!event.SerializeToBuffer(ptr + header.PackLength(),
                               header.event_length - header.PackLength()))
    return -1;
  return 1;
}

BinlogEncryptor *NullBinlogEncryptor::Copy() const {
  return new NullBinlogEncryptor();
}

file_util::ReadResultCode NullBinlogEncryptor::Read(file::InputFile *file,
                                                    off_t end_of_file,
                                                    Buffer *dst) {
  int64_t offset;
  file->Tell(&offset);
  if (offset + constants::LOG_EVENT_HEADER_LENGTH > end_of_file) {
    return file_util::READ_EOF;
  }

  dst->clear();
  if (!file->Read(*dst, constants::LOG_EVENT_HEADER_LENGTH)) {
    LOG(WARNING) << "Failed to read unencrypted log event"
                 << ", offset: " << offset
                 << ", len: " << constants::LOG_EVENT_HEADER_LENGTH;
    return file_util::READ_ERROR;
  }

  LogEventHeader header;
  if (!header.ParseFromBuffer(dst->data(), dst->size())) {
    LOG(WARNING) << "Failed to read unencrypted log event"
                 << ", offset: " << offset << ", LogEventHeader.Parse failed";
    return file_util::READ_ERROR;
  }

  int length = header.event_length;
  int remaining = length - dst->size();
  if (offset + length > end_of_file) {
    LOG(WARNING) << "Failed to read unencrypted log event"
                 << ", offset: " << offset
                 << ", length: " << length
                 << ", end_of_file: " << end_of_file;
    return file_util::READ_EOF;
  }

  if (remaining != 0 && !file->Read(*dst, remaining)) {
    LOG(WARNING) << "Failed to read unencrypted log event"
                 << ", offset: " << offset << ", remaining: " << remaining;
    return file_util::READ_ERROR;
  }
  return file_util::READ_OK;
}

bool NullBinlogEncryptor::Write(file::AppendOnlyFile *file,
                                absl::string_view src) {
  int64_t offset;
  file->Tell(&offset);
  bool ok = file->Write(src);
  if (!ok) {
    LOG(WARNING) << "Failed to write unencrypted log event"
                 << ", offset: " << offset << ", len: " << src.size();
  }
  return ok;
}

BinlogEncryptor* BinlogEncryptorFactory::GetInstance(int crypt_scheme) {
  switch (crypt_scheme) {
    case 0:
      return new NullBinlogEncryptor();
    case 1:
    case 255:
      if (crypt_scheme == 1 && FLAGS_danger_danger_use_dbug_keys)
        crypt_scheme = 255;
      // 255 = example key handler
      return new AesGcmBinlogEncryptor(
          crypt_scheme,
          KeyHandler::GetInstance(crypt_scheme == 255));
  }
  return nullptr;
}

BinlogEncryptor *BinlogEncryptorFactory::GetInstance(
    RawLogEventData raw_event) {
  assert(raw_event.header.type == constants::ET_START_ENCRYPTION);
  if (raw_event.header.type != constants::ET_START_ENCRYPTION) {
    return nullptr;
  }

  StartEncryptionEvent event;
  if (!ParseFromRawLogEventData(&event, raw_event)) {
    assert(false);
    return nullptr;
  }

  if (event.crypt_scheme == 0) {
    return new NullBinlogEncryptor();
  }

  if (!(event.crypt_scheme == 1 || event.crypt_scheme == 255)) {
    assert(false);
    return nullptr;
  }

  if (event.crypt_scheme == 1 && FLAGS_danger_danger_use_dbug_keys)
    event.crypt_scheme = 255;

  // scheme 255 => example key handler...
  KeyHandler *key_handler = KeyHandler::GetInstance(event.crypt_scheme == 255);
  AesGcmBinlogEncryptor *aes = new AesGcmBinlogEncryptor(event.crypt_scheme,
                                                         key_handler);
  if (!aes->SetKeyAndNonce(event.key_version,
                           reinterpret_cast<const uint8_t*>(event.nonce.data()),
                           event.nonce.size())) {
    delete aes;
    return nullptr;
  }

  return aes;
}

}  // namespace mysql_ripple
