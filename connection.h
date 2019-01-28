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

#ifndef MYSQL_RIPPLE_CONNECTION_H
#define MYSQL_RIPPLE_CONNECTION_H

#include <string>

#include "absl/synchronization/mutex.h"

namespace mysql_ripple {

// A class representing a connection from/to ripple
class Connection {
 public:
  virtual ~Connection();

  enum ConnectionStatus {
    ERROR_STATUS,
    DISCONNECTED,                // when we start, when we end
    CONNECTING,                  // between connect() and first recv()
    DISCONNECTING,               // disconnect is in progress
    CONNECT_FAILED,              // connect failed and we are disconnected
    CONNECTED,                   //
    ABORTING                     // Abort() has been called but not "handled"
  };

  // The following function is used for monitoring purposes:
  static std::string to_string(ConnectionStatus status) {
    switch (status) {
      case ERROR_STATUS:   return "ERROR_STATUS";
      case DISCONNECTED:   return "DISCONNECTED";
      case CONNECTING:     return "CONNECTING";
      case DISCONNECTING:  return "DISCONNECTING";
      case CONNECT_FAILED: return "CONNECT_FAILED";
      case CONNECTED:      return "CONNECTED";
      case ABORTING:       return "ABORTING";
      default:             return "<unknown>";
    }
  }

  virtual ConnectionStatus connection_status() const {
    absl::MutexLock lock(&mutex_);
    return connection_status_;
  }

  enum ConnectionType {
    MYSQL_CLIENT_CONNECTION,    // A connection from ripple to mysqld
    MYSQL_SERVER_CONNECTION     // A connection to ripple
  };

  virtual ConnectionType connection_type() const {
    return connection_type_;
  }

  // Disconnect.
  // This method blocks.
  virtual void Disconnect() = 0;

  // Abort a connection.
  // This method aborts a connection that is CONNECTING or CONNECTED.
  // This method is non blocking and returns true if the connection was
  // in correct state and false otherwise. The "socket implementation" of this
  // is shutdown(2).
  virtual void Abort() = 0;

  // Check if there is a latest error.
  virtual bool HasError() const;

  // Get last error message.
  // This message is reset before each method called on this class
  // except HasError and GetLastErrorMessage.
  virtual std::string GetLastErrorMessage() const;

  // A packet.
  struct Packet {
    int length;
    const uint8_t *ptr;
  };

  // Monitoring Information
  virtual std::string GetHost() const = 0;
  virtual uint16_t GetPort() const = 0;
  virtual uint64_t GetBytesSent() const { return bytes_sent; }
  virtual uint64_t GetBytesReceived() const { return bytes_received; }

 protected:
  // Constructor for subclass
  explicit Connection(ConnectionType);

  mutable absl::Mutex mutex_;
  ConnectionStatus connection_status_;
  const ConnectionType connection_type_;
  mutable std::string last_error_message_;

  uint64_t bytes_sent;
  uint64_t bytes_received;

  void ClearError() const { last_error_message_.clear(); }
  void SetError(const std::string &msg) const;
};

}  // namespace mysql_ripple

#endif  // MYSQL_RIPPLE_CONNECTION_H
