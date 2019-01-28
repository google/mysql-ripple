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

#include "gtest/gtest.h"

#include "file.h"
#include "log_event.h"
#include "mysql_constants.h"

namespace mysql_ripple {

TEST(KeyHandler, Basic) {
  KeyHandler *handler = KeyHandler::GetInstance(true);

  uint8_t buf[16];
  EXPECT_NE(handler->GetLatestKeyVersion(), 0);
  EXPECT_TRUE(handler->GetKeyBytes(handler->GetLatestKeyVersion(),
                                   buf, sizeof(buf)));
  EXPECT_TRUE(handler->GetKeyBytes(handler->GetLatestKeyVersion(),
                                   buf, sizeof(buf) - 1));
  EXPECT_TRUE(handler->GetRandomBytes(buf, sizeof(buf)));
  EXPECT_TRUE(handler->GetRandomBytes(buf, sizeof(buf) - 1));
  delete handler->Copy();  // get coverage of Copy()-method

  delete handler;
}

TEST(BinlogEncryptorFactory, Basic) {
  std::vector<int> list = {
    0,   // NullBinlogEncryptor
    255  // Aes+ExampleKeyHandler
  };
  for (auto crypt_scheme : list) {
    BinlogEncryptor *enc = BinlogEncryptorFactory::GetInstance(crypt_scheme);
    enc->Init();
    EXPECT_TRUE(enc != nullptr);
    delete enc->Copy();  // get coverage of Copy()-method
    delete enc;
  }

  {
    BinlogEncryptor *enc = BinlogEncryptorFactory::GetInstance(256);
    EXPECT_TRUE(enc == nullptr);
  }
}

TEST(AesGcmBinlogEncryptor, Basic) {
  AesGcmBinlogEncryptor encryptor(255, KeyHandler::GetInstance(true));
  // incorrect nonce length
  EXPECT_FALSE(encryptor.SetKeyAndNonce(0, nullptr, 0));
  EXPECT_TRUE(encryptor.Init());

  // Create a fake event (random bytes)
  Buffer event;
  event.Append(38);
  EXPECT_TRUE(encryptor.GetRandomBytes(event.data(), event.size()));

  int64_t offset = time(0);

  // Encrypt same event twice, once @offset and once @offset+1.
  Buffer enc1;
  EXPECT_TRUE(encryptor.Encrypt(offset, event.data(), event.size(), &enc1));
  EXPECT_EQ(enc1.size(), event.size() + encryptor.GetExtraSize());

  Buffer enc2;
  EXPECT_TRUE(encryptor.Encrypt(offset + 1, event.data(), event.size(), &enc2));
  EXPECT_EQ(enc2.size(), event.size() + encryptor.GetExtraSize());

  // This should be different encrypted data.
  EXPECT_EQ(enc1.size(), enc2.size());
  EXPECT_NE(memcmp(enc1.data(), enc2.data(), enc1.size()), 0);

  // This should fail since offset is wrong, tag check should fail).
  Buffer fail1;
  EXPECT_FALSE(encryptor.Decrypt(offset + 1, enc1.data(), enc1.size(), &fail1));
  EXPECT_FALSE(encryptor.Decrypt(offset, enc2.data(), enc2.size(), &fail1));

  // This should fail length is wrong.
  EXPECT_FALSE(encryptor.Decrypt(offset, enc1.data(), enc1.size()+1, &fail1));

  // Check that we get same data back
  {
    Buffer check1;
    EXPECT_TRUE(encryptor.Decrypt(offset, enc1.data(), enc1.size(), &check1));
    EXPECT_EQ(check1.size(), event.size());
    EXPECT_EQ(memcmp(event.data(), check1.data(), event.size()), 0);
  }

  // Check that we get same data back
  {
    Buffer check2;
    EXPECT_TRUE(encryptor.Decrypt(offset + 1, enc2.data(), enc2.size(),
                                  &check2));
    EXPECT_EQ(check2.size(), event.size());
    EXPECT_EQ(memcmp(event.data(), check2.data(), event.size()), 0);
  }
}

const char *test_dir = nullptr;
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

void TestReadWrite(BinlogEncryptor *encryptor, const file::Factory &factory) {
  std::string name =
      std::string(GetTestDir()) + "/test." + std::to_string(getpid());
  file::AppendOnlyFile *ofile;
  file::InputFile *ifile;
  ASSERT_TRUE(factory.Open(&ofile, name, "w"));
  ASSERT_TRUE(factory.Open(&ifile, name, "r"));

  /**
   * create a list of fake events...
   * - random length and content == length
   */
  unsigned seed = time(0);
  std::vector<int> events;
  for (int i = 0; i < 256; i++) {
    events.push_back(constants::LOG_EVENT_HEADER_LENGTH +
                     (rand_r(&seed) % 1024));
  }

  for (int size : events) {
    int val = (size & 255);

    Buffer buf;
    buf.resize(size);
    memset(buf.data() + constants::LOG_EVENT_HEADER_LENGTH, val,
           size - constants::LOG_EVENT_HEADER_LENGTH);
    LogEventHeader header;
    memset(&header, 0, sizeof(header));
    header.event_length = size;
    header.type = val;
    header.SerializeToBuffer(buf.data(), constants::LOG_EVENT_HEADER_LENGTH);
    EXPECT_TRUE(encryptor->Write(ofile, buf.data(), buf.size()));
    EXPECT_TRUE(ofile->Flush());
  }

  int64_t end_of_file;
  ofile->Tell(&end_of_file);
  EXPECT_TRUE(ifile->Seek(0));
  for (int size : events) {
    int val = (size & 255);

    Buffer buf;
    EXPECT_EQ(encryptor->Read(ifile, end_of_file, &buf), file_util::READ_OK);
    EXPECT_EQ(buf.size(), size);
    bool ok = true;
    for (int i = constants::LOG_EVENT_HEADER_LENGTH; i < buf.size(); i++) {
      if (ok && (buf.data()[i] != val)) {
        printf("buf.data[%d] == %d (expect: %d)\n", i, buf.data()[i], val);
        ok = false;
      }
    }
    EXPECT_TRUE(ok);
    LogEventHeader header;
    EXPECT_TRUE(header.ParseFromBuffer(buf.data(), buf.size()));
  }

  {
    Buffer buf;
    EXPECT_EQ(encryptor->Read(ifile, end_of_file, &buf), file_util::READ_EOF);
  }

  ASSERT_TRUE(ofile->Close());
  ASSERT_TRUE(ifile->Close());
  ASSERT_TRUE(factory.Delete(name));
}

TEST(AesGcmBinlogEncryptor, ReadWriteFILE) {
  AesGcmBinlogEncryptor encryptor(255, KeyHandler::GetInstance(true));
  EXPECT_TRUE(encryptor.Init());
  TestReadWrite(&encryptor, file::FILE_Factory());
}

TEST(NullBinlogEncryptor, ReadWriteFILE) {
  NullBinlogEncryptor encryptor;
  TestReadWrite(&encryptor, file::FILE_Factory());
}

TEST(AesGcmBinlogEncryptor, LogEvent) {
  AesGcmBinlogEncryptor encryptor(255, KeyHandler::GetInstance(true));
  encryptor.Init();

  Buffer buf;
  LogEventHeader header;
  memset(&header, 0, sizeof(header));
  EXPECT_EQ(encryptor.GetStartEncryptionEvent(header, &buf), 1);

  RawLogEventData event;
  EXPECT_TRUE(event.ParseFromBuffer(buf.data(), buf.size()));

  BinlogEncryptor *encryptor2 = BinlogEncryptorFactory::GetInstance(event);
  EXPECT_TRUE(encryptor2 != nullptr);

  // check that the encryptors are "the same", by encrypting an event
  // gives same byte sequence
  unsigned int seed = 42;
  int64_t offset = rand_r(&seed);

  Buffer encrypted_event1;
  encryptor.Encrypt(offset, buf.data(), buf.size(), &encrypted_event1);

  Buffer encrypted_event2;
  reinterpret_cast<AesGcmBinlogEncryptor*>(encryptor2)->Encrypt(
      offset, buf.data(), buf.size(), &encrypted_event2);

  EXPECT_EQ(encrypted_event1.size(), encrypted_event2.size());
  EXPECT_EQ(memcmp(encrypted_event1.data(), encrypted_event2.data(),
                   encrypted_event1.size()),
            0);

  delete encryptor2;
}

}  // namespace mysql_ripple
