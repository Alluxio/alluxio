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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.StreamCapabilities;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * HdfsFileInputStream implement for hadoop 3.
 * This is just a wrapper around {@link HdfsFileInputStream} with
 * CanUnbuffer and StreamCapabilities support.
 */
@NotThreadSafe
public class HdfsFileInputStream extends BaseHdfsFileInputStream
    implements CanUnbuffer, StreamCapabilities {
  /**
   * Constructs a new stream for reading a file from HDFS.
   *
   * @param fs the file system
   * @param uri the Alluxio file URI
   * @param stats filesystem statistics
   */
  public HdfsFileInputStream(FileSystem fs, AlluxioURI uri, Statistics stats)
      throws IOException {
    super(fs, uri, stats);
  }

  /**
   * Constructs a new stream for reading a file from HDFS.
   *
   * @param inputStream the input stream
   * @param stats filesystem statistics
   */
  public HdfsFileInputStream(FileInStream inputStream, Statistics stats) {
    super(inputStream, stats);
  }

  @Override
  public boolean hasCapability(String capability) {
    return StringUtils.equalsIgnoreCase("in:unbuffer", capability)
        || StringUtils.equalsIgnoreCase("in:readbytebuffer", capability);
  }

  @Override
  public void unbuffer() {
    mInputStream.unbuffer();
  }
}
