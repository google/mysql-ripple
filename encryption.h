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

#ifndef MYSQL_RIPPLE_ENCRYPTION_H
#define MYSQL_RIPPLE_ENCRYPTION_H

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>

#include "absl/strings/string_view.h"
#include "buffer.h"
#include "file.h"
#include "file_util.h"
#include "log_event.h"

namespace mysql_ripple {

class KeyHandler {
 public:
  // Return a KeyHandler.
  // Should be deleted by caller.
  static KeyHandler* GetInstance(bool example);

  virtual ~KeyHandler() {}

  // Make copy of this key handler.
  // Used when copying encryptor state (BinlogEncryptor::Copy()).
  virtual KeyHandler *Copy() const;

  virtual uint32_t GetLatestKeyVersion();

  // Get bytes for key_version and store them in buffer of length.
  virtual bool GetKeyBytes(uint32_t key_version, uint8_t *buffer, int length);

  // Get length bytes of random data and store them in buffer.
  virtual bool GetRandomBytes(uint8_t *buffer, int length);
 protected:
  KeyHandler() {}
};

// Abstract interface for binlog encryption.
class BinlogEncryptor {
 public:
  virtual ~BinlogEncryptor() {}

  // Reinitialize encryptor for a new file.
  // For AesGcmBinlogEncryptor this gets a new key and generates new nonce.
  // For NullBinlogEncryptor this does nothing.
  virtual bool Init() = 0;

  // Make copy of this encryptor.
  virtual BinlogEncryptor *Copy() const = 0;

  // Read one event from file and store it into dst.
  // Event is read from current file position and no attempt to read after
  // end_of_file will be made.
  virtual file_util::ReadResultCode Read(file::InputFile *file,
                                         off_t end_of_file, Buffer *dst) = 0;

  // Write one event to file at current position.
  virtual bool Write(file::AppendOnlyFile *file, absl::string_view src) = 0;

  bool Write(file::AppendOnlyFile *file, const uint8_t *src, int len) {
    return Write(file,
                 absl::string_view(reinterpret_cast<const char *>(src), len));
  }

  // Get extra size needed for the encryption.
  virtual int GetExtraSize() const = 0;

  // Get StartEncryption event
  // return 0 if none needed
  //        1 if event added to dst
  //       -1 on error
  virtual int GetStartEncryptionEvent(const LogEventHeader& header,
                                      Buffer *dst) const = 0;
};

class AesGcmBinlogEncryptor : public BinlogEncryptor {
 public:
  // Note that AesGcmBinlogEncryptor takes ownership of KeyHandler
  AesGcmBinlogEncryptor(int crypt_scheme, KeyHandler *key_handler);
  AesGcmBinlogEncryptor(const AesGcmBinlogEncryptor&);

  virtual ~AesGcmBinlogEncryptor() {}

  // GetRandomBytes from KeyHandler (convenience).
  virtual bool GetRandomBytes(uint8_t *buffer, int length) {
    return key_handler_->GetRandomBytes(buffer, length);
  }

  // Reinitialize key version and nonce.
  // Used when create a new binlog file.
  bool Init() override;

  // Make copy of this encryptor.
  BinlogEncryptor *Copy() const override;

  // Set key version and nonce, used when reading it from binlog.
  virtual bool SetKeyAndNonce(uint32_t version, const uint8_t *nonce, int len);

  // Encrypt or decrypt one event.
  virtual bool Encrypt(off_t pos, const uint8_t *src, int len, Buffer *dst);
  virtual bool Decrypt(off_t pos, const uint8_t *src, int len, Buffer *dst);

  // Get extra size needed for the encryption.
  int GetExtraSize() const override;

  // Read one event from file and store it into dst.
  // Event is read from current file position and no attempt to read after
  // end_of_file will be made.
  file_util::ReadResultCode Read(file::InputFile *file, off_t end_of_file,
                                 Buffer *dst) override;

  // Write one event to file at current position.
  bool Write(file::AppendOnlyFile *file, absl::string_view src) override;

  // Get StartEncryption event
  // return 0 if none needed
  //        1 if event added to dst
  //       -1 on error
  int GetStartEncryptionEvent(const LogEventHeader& header,
                              Buffer *dst) const override;

 private:
  Buffer iv_;
  Buffer key_;
  Buffer buffer_;
  int crypt_scheme_;
  uint32_t key_version_;
  std::unique_ptr<KeyHandler> key_handler_;
};

class NullBinlogEncryptor : public BinlogEncryptor {
 public:
  virtual ~NullBinlogEncryptor() {}
  bool Init() override { return true; }

  // Make copy of this encryptor.
  BinlogEncryptor *Copy() const override;

  // Read one event from file and store it into dst.
  // Event is read from current file position and no attempt to read after
  // end_of_file will be made.
  file_util::ReadResultCode Read(file::InputFile *file, off_t end_of_file,
                                 Buffer *dst) override;

  // Write one event to file at current position.
  bool Write(file::AppendOnlyFile *file, absl::string_view src) override;

  // Get extra size needed for the encryption.
  int GetExtraSize() const override { return 0; }

  // Get StartEncryption event.
  // return 0 if none needed
  //        1 if event added to dst
  //       -1 on error
  int GetStartEncryptionEvent(const LogEventHeader& header,
                              Buffer *dst) const override {
    return 0;
  }
};

class BinlogEncryptorFactory {
 public:
  // Get binlog encryptor for crypt_scheme.
  // Returned object shall be freed by caller.
  static BinlogEncryptor *GetInstance(int crypt_scheme);
  static BinlogEncryptor *GetInstance(RawLogEventData);
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_ENCRYPTION_H
