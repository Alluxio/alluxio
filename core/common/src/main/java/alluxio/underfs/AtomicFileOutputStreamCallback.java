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

import alluxio.underfs.options.CreateOptions;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link AtomicFileOutputStreamCallback} is the callback interface used when using a
 * {@link AtomicFileOutputStream}.
 */
public interface AtomicFileOutputStreamCallback extends UnderFileSystem {
  /**
   * Creates a file in the under file system with the specified {@link CreateOptions}. This stream
   * writes directly to the underlying storage without any atomicity guarantees.
   *
   * @param path the file name
   * @param options the options for create
   * @return A {@code OutputStream} object
   */
  OutputStream createDirect(String path, CreateOptions options) throws IOException;
}

