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

#ifndef FILEINSTREAM_H
#define FILEINSTREAM_H

#include "JNIHelper.h"

using namespace jnihelper;

namespace alluxio {

// A streaming API to read a file. This API represents a file as a stream of
// bytes and provides a collection of read methods to access this stream of
// bytes.
class FileInStream {

 public:
  FileInStream(jobject alluxioInStream);
  ~FileInStream();
  // Reads one byte to b
  Status Read(char* b);
  // Reads len bytes into buf starting offset off of length len,
  Status Read(char* buf, size_t off, size_t len, size_t* result);
  // Moves the starting read position of the stream to the specified position
  // which is relative to the start of the stream. Seeking to a position before
  // the current read position is supported.
  Status Seek(size_t pos);
  // Skips over and discards <code>n</code> bytes of data from this input
  // stream
  Status Skip(size_t pos);
  // Closes this input stream
  Status Close();

 private:
  jobject inStream;
};

} // namespace alluxio

#endif // FILEINSTREAM_H
