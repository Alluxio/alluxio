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

package alluxio.table.under.hive.parquet;

import alluxio.client.file.FileInStream;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Alluxio SeekableInputStream implementation.
 */
public class AlluxioSeekableInputStream extends DelegatingSeekableInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSeekableInputStream.class);

  private final FileInStream mInStream;

  private AlluxioSeekableInputStream(FileInStream inStream) {
    super(inStream);
    mInStream = inStream;
  }

  /**
   * Creates an instance.
   *
   * @param inStream the alluxio file instream
   * @return the new instance
   */
  public static AlluxioSeekableInputStream create(FileInStream inStream) {
    return new AlluxioSeekableInputStream(inStream);
  }

  @Override
  public long getPos() throws IOException {
    return mInStream.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    mInStream.seek(newPos);
  }
}
