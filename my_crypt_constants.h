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

#ifndef MYSYS_MY_CRYPT_CONSTANTS_H_
#define MYSYS_MY_CRYPT_CONSTANTS_H_

enum CryptResult {
  CRYPT_OK = 0,
  CRYPT_BAD_IV,
  CRYPT_INVALID,
  CRYPT_OPENSSL_ERROR
};

enum {
  CRYPT_DECRYPT = 0,
  CRYPT_ENCRYPT = 1
};

// AES-128(bit) constants
#define AES_128_BLOCK_SIZE 16
#define AES_128_CTR_NONCE_SIZE 8

// MySQLd crypto schemes
enum CryptScheme {
  CRYPT_SCHEME_UNENCRYPTED = 0,
  // crypto scheme 1 is:
  // - AES-128-GCM for BIN log encryption
  // - AES-128-CTR for SQL log encryption
  CRYPT_SCHEME_1 = 1
};

// SQL log crypt constants.

// Magic bytes for an encrypted sql log file.
// Require they are not text char or number to not conflict with ip or
// hostname
extern const unsigned char SQL_LOG_CRYPT_MAGIC[4];
// Encrypted SQL log file header size. 36 bytes total.
static const int SQL_LOG_HEADER_SIZE =
    4 +  // SQL_LOG_CRYPT_MAGIC size
    4 +  // mysqld crypt scheme size
    4 +  // mysqld crypt key version size
    AES_128_BLOCK_SIZE +
    AES_128_CTR_NONCE_SIZE;

#endif  // MYSYS_MY_CRYPT_CONSTANTS_H_
