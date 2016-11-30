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

import alluxio.Constants;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link org.apache.hadoop.fs.FileSystem}
 * interface. Any program working with Hadoop HDFS can work with Alluxio transparently. Note that
 * the performance of using this fault tolerant API may not be as efficient as the performance of
 * using the Alluxio native API defined in {@link alluxio.client.file.FileSystem}, which the API is
 * built on top of.
 *
 * <p>
 * Unlike {@link FileSystem}, this class enables Zookeeper.
 * </p>
 */
@NotThreadSafe
public final class FaultTolerantFileSystem extends AbstractFileSystem {

  /**
   * Constructs a new {@link FaultTolerantFileSystem}.
   */
  public FaultTolerantFileSystem() {
    super();
  }

  /**
   * Constructs a new {@link FaultTolerantFileSystem} instance with a specified
   * {@link alluxio.client.file.FileSystem} handler for tests.
   *
   * @param fileSystem handler to file system
   */
  public FaultTolerantFileSystem(alluxio.client.file.FileSystem fileSystem) {
    super(fileSystem);
  }

  @Override
  public String getScheme() {
    return Constants.SCHEME_FT;
  }

  @Override
  protected boolean isZookeeperMode() {
    return true;
  }
}
