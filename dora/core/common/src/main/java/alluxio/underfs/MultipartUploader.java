/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The multipart uploader interface to support multipart uploading.
 * The interface is inspired by hadoop {@link org.apache.hadoop.fs.impl.FileSystemMultipartUploader}
 */
public interface MultipartUploader {
  /**
   * Initialize a multipart upload.
   * @throws IOException IO failure
   */
  void startUpload() throws IOException;

  /**
   * Put part as part of a multipart upload.
   * It is possible to have parts uploaded in any order (or in parallel).
   * stream after reading in the data.
   * @param b the byte array to put. The byte buffer must have been flipped and be ready to read
   * @param partNumber the part number of this file part
   * @return a future of the async upload task
   * @throws IOException IO failure
   */
  ListenableFuture<Void> putPart(ByteBuffer b, int partNumber)
      throws IOException;

  /**
   * Complete a multipart upload.
   * @throws IOException IO failure
   */
  void complete() throws IOException;

  /**
   * Aborts a multipart upload.
   * @throws IOException IO failure
   */
  void abort() throws IOException;

  /**
   * Wait for the ongoing uploads to complete.
   * @throws IOException IO failure
   */
  void flush() throws IOException;
}
