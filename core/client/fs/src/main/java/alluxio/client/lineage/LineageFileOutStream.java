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

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream API to write a file when lineage is enabled. It supports asynchronous persistence of the
 * data to the under storage system.
 */
@PublicApi
@NotThreadSafe
public class LineageFileOutStream extends FileOutStream {
  /**
   * Creates a new file output stream when lineage is enabled.
   *
   * @param context file system context
   * @param path the path of the file
   * @param options the set of options specific to this operation
   */
  public LineageFileOutStream(FileSystemContext context, AlluxioURI path, OutStreamOptions options)
      throws IOException {
    super(path, updateOutStreamOptions(options), context);
  }

  /**
   * Sets the {@link WriteType} to {@link WriteType#ASYNC_THROUGH} for the options.
   *
   * @param options the set of options
   * @return the updated options
   */
  private static OutStreamOptions updateOutStreamOptions(OutStreamOptions options) {
    return options.setWriteType(WriteType.ASYNC_THROUGH);
  }

  @Override
  protected void scheduleAsyncPersist() throws IOException {
    // do nothing, the scheduling is handled by the lineage master
  }
}
