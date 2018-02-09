/**
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

#ifndef CPP_INCLUDE_STATUS_H_
#define CPP_INCLUDE_STATUS_H_

#include <assert.h>
#include <cstring>
#include <string>

// The returned Status of FileSystem operation
class Status {
 public:
  // Creates a success status
  Status() : state_(NULL) { }
  ~Status() {
    delete[] state_;
  }
  // Copys the specified status
  Status(const Status& s);

  void operator = (const Status& s);

  static Status OK() {
    return Status();  // return success status
  }

  static Status aborted(const std::string& msg) {
    return Status(ABORTED, msg);
  }

  static Status alreadyExist(const std::string& msg) {
    return Status(ALREADY_EXISTS, msg);
  }

  static Status canceled(const std::string& msg) {
    return Status(CANCELED, msg );
  }

  static Status dataLoss(const std::string& msg) {
    return Status(DATA_LOSS, msg);
  }

  static Status deadlineExceeded(const std::string& msg) {
    return Status(DEADLINE_EXCEEDED, msg);
  }

  static Status failedPrecondition(const std::string& msg) {
    return Status(FAILED_PRECONDITION, msg);
  }

  static Status internal(const std::string& msg) {
    return Status(INTERNAL, msg);
  }

  static Status invalidArgument(const std::string& msg) {
    return Status(INVALID_ARGUMENT, msg);
  }

  static Status notFound(const std::string& msg) {
    return Status(NOT_FOUND, msg);
  }

  static Status outOfRange(const std::string& msg) {
    return Status(OUT_OF_RANGE, msg);
  }

  static Status permissionDenied(const std::string& msg) {
    return Status(PERMISSION_DENIED, msg);
  }

  static Status unAuthenticated(const std::string& msg) {
    return Status(UNAUTHENTICATED, msg);
  }

  static Status resourceExhausted(const std::string& msg) {
    return Status(RESOURCE_EXHAUSTED, msg);
  }

  static Status unavailable(const std::string& msg) {
    return Status(UNAVAILABLE, msg);
  }

  static Status unImplemented(const std::string& msg) {
    return Status(UNIMPLEMENTED, msg);
  }

  static Status unknown(const std::string& msg) {
    return Status(UNKNOWN, msg);
  }

  static Status jniError(const std::string& msg) {
    return Status(JNI_ERROR, msg);
  }

  bool ok() const {
    return (state_ == NULL);
  }

  // Returns a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

 private:
  // OK status has a NULL state_. Otherwise, state_ is a new[] array
  // of the following form:
  //  state_[0..3] == length of message
  //  state_[4]  == code
  //  state_[5..] == message
  const char* state_;

  enum Code {
    SUCCEED = 0,
    ABORTED = 1,
    ALREADY_EXISTS = 2,
    CANCELED = 3,
    DATA_LOSS = 4,
    DEADLINE_EXCEEDED = 5,
    FAILED_PRECONDITION = 6,
    INTERNAL = 7,
    INVALID_ARGUMENT = 8,
    JNI_ERROR = 9,
    NOT_FOUND = 10,
    OUT_OF_RANGE = 11,
    PERMISSION_DENIED = 12,
    RESOURCE_EXHAUSTED = 13,
    UNAUTHENTICATED = 14,
    UNAVAILABLE = 15,
    UNIMPLEMENTED = 16,
    UNKNOWN = 17
  };

  Code code() const {
    return (state_ == NULL) ? SUCCEED : static_cast<Code>(state_[4]);
  }

  Status(Code code, const std::string& msg);
  static const char* CopyState(const char* s);
};

#endif  // CPP_INCLUDE_STATUS_H_
