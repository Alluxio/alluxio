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

import org.apache.hadoop.fs.FileSystem.Statistics;

import java.io.IOException;

/**
 * HdfsFileInputStream implement for hadoop 1 and hadoop 2.
 */
public class HdfsFileInputStream extends BaseHdfsFileInputStream {

  /**
   * Constructs a new stream for reading a file from HDFS.
   *
   * @param fs the file system
   * @param uri the Alluxio file URI
   * @param stats filesystem statistics
   */
  public HdfsFileInputStream(FileSystem fs, AlluxioURI uri, Statistics stats) throws IOException {
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
}
