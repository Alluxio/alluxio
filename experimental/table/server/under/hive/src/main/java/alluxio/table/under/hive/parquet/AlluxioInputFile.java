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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Alluxio InputFile implementation.
 */
public class AlluxioInputFile implements InputFile {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioInputFile.class);

  private final FileSystem mFs;
  private final URIStatus mStatus;

  private AlluxioInputFile(FileSystem fs, URIStatus status) {
    mFs = fs;
    mStatus = status;
  }

  /**
   * Creates instance.
   *
   * @param fs the alluxio fs
   * @param status the uri status of the file
   * @return the new instance
   */
  public static AlluxioInputFile create(FileSystem fs, URIStatus status) {
    return new AlluxioInputFile(fs, status);
  }

  @Override
  public long getLength() throws IOException {
    return mStatus.getLength();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    try {
      return AlluxioSeekableInputStream.create(mFs.openFile(new AlluxioURI(mStatus.getPath())));
    } catch (AlluxioException e) {
      throw new IOException("Failed to open alluxio path: " + mStatus.getPath(), e);
    }
  }
}
