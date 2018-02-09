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

#ifndef CPP_INCLUDE_FILEOUTSTREAM_H_
#define CPP_INCLUDE_FILEOUTSTREAM_H_

#include "jniHelper.h"

using ::jnihelper::JniHelper;

namespace alluxio {
// Provides a streaming API to write a file
class FileOutStream {
 public:
  explicit FileOutStream(jobject AlluxioOutStream);
  ~FileOutStream();
  // Writes the specified byte to this output stream
  Status Write(char b);
  // Writes len bytes from the specified byte array
  // starting at offset to this output stream.
  Status Write(const char* buf, size_t off, size_t len);
  // Flushes this output stream and forces any buffered output bytes
  // to be written out.
  Status Flush();
  // Cancels write operation, closing current stream
  Status Cancel();
  // Closes current stream
  Status Close();

 private:
  jobject outStream;
};

}   // namespace alluxio

#endif  // CPP_INCLUDE_FILEOUTSTREAM_H_
