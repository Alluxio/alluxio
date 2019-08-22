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
import alluxio.annotation.PublicApi;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;

import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.URI;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link org.apache.hadoop.fs.FileSystem}
 * interface. Any program working with Hadoop HDFS can work with Alluxio transparently. Note that
 * the performance of using this API may not be as efficient as the performance of using the Alluxio
 * native API defined in {@link alluxio.client.file.FileSystem}, which this API is built on top of.
 *
 * ShimFileSystem supports working with arbitrary schemes that are supported by Alluxio.
 */
@PublicApi
@NotThreadSafe
public class ShimFileSystem extends AbstractFileSystem {
  /**
   * Constructs a new {@link ShimFileSystem}.
   */
  public ShimFileSystem() {
    super();
  }

  /**
   * Constructs a new {@link ShimFileSystem} instance with a specified
   * {@link alluxio.client.file.FileSystem} handler for tests.
   *
   * @param fileSystem handler to file system
   */
  public ShimFileSystem(alluxio.client.file.FileSystem fileSystem) {
    super(fileSystem);
  }

  @Override
  public String getScheme() {
    //
    // {@link #getScheme()} will be used in hadoop 2.x for dynamically loading
    // filesystems based on scheme. This limits capability of ShimFileSystem
    // as it's intended to be a forwarder for arbitrary schemes.
    //
    // Hadoop currently gives configuration priority over dynamic loading, so
    // whatever scheme is configured for ShimFileSystem will be attached with a shim.
    // Below constant will basically hide ShimFileSystem from dynamic loading as
    // it maps to a bogus scheme.
    //
    return Constants.NO_SCHEME;
  }

  @Override
  protected boolean isZookeeperMode() {
    return mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
  }

  @Override
  // Gets the full path to use when contacting Alluxio server.
  protected String getAlluxioPath(Path path) {
    return path.toString();
  }

  @Override
  // Gets UFS path as native FS path.
  protected Path getFsPath(URIStatus fileStatus) {
    return new Path(fileStatus.getUfsPath());
  }

  @Override
  protected void validateFsUri(URI fsUri) throws IOException {
    // No validation for ShimFS.
  }
}
