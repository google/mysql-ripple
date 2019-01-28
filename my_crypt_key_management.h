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


#ifndef MYSYS_MY_CRYPT_KEY_MANAGMENT_H_
#define MYSYS_MY_CRYPT_KEY_MANAGMENT_H_

#include "my_global.h"
#include "my_pthread.h"
#include "mysql/psi/psi.h"

extern my_bool opt_danger_danger_use_dbug_keys;

#ifndef DBUG_OFF
#ifdef HAVE_PSI_INTERFACE
extern PSI_rwlock_key key_LOCK_dbug_crypto_key_version;
#endif

extern mysql_rwlock_t LOCK_dbug_crypto_key_version;
extern uint opt_danger_danger_dbug_crypto_key_version;
#endif

C_MODE_START
/**
 * function returning latest key version
 */
typedef unsigned int (* GetLatestCryptoKeyVersionFunc_t)();

/**
 * function returning a key for a key version
 */
typedef int (* GetCryptoKeyFunc_t)(unsigned int version,
                                   unsigned char* key,
                                   unsigned keybufsize);


struct CryptoKeyFuncs_t
{
  GetLatestCryptoKeyVersionFunc_t getLatestCryptoKeyVersionFunc;
  GetCryptoKeyFunc_t getCryptoKeyFunc;
};

/**
 * Install functions to use for key management
 */
void
InstallCryptoKeyFunctions(const struct CryptoKeyFuncs_t* cryptoKeyFuncs);

/**
 * Functions to interact with key management
 */

unsigned int GetLatestCryptoKeyVersion();
int GetCryptoKey(unsigned int version, unsigned char* key_buffer,
                 unsigned int size);

C_MODE_END

#endif // MYSYS_MY_CRYPT_KEY_MANAGMENT_H_
