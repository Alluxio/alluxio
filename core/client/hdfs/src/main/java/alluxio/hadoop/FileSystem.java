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
import alluxio.Constants;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.exception.PreconditionMessage;
import alluxio.uri.Authority;
import alluxio.uri.UnknownAuthority;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.net.URI;

/**
 * An Alluxio client API compatible with Apache Hadoop {@link org.apache.hadoop.fs.FileSystem}
 * interface. Any program working with Hadoop HDFS can work with Alluxio transparently. Note that
 * the performance of using this API may not be as efficient as the performance of using the Alluxio
 * native API defined in {@link alluxio.client.file.FileSystem}, which this API is built on top of.
 */
@PublicApi
@NotThreadSafe
public final class FileSystem extends AbstractFileSystem {
  /**
   * Constructs a new {@link FileSystem}.
   */
  public FileSystem() {
    super();
  }

  /**
   * Constructs a new {@link FileSystem} instance with a
   * specified {@link alluxio.client.file.FileSystem} handler for tests.
   *
   * @param fileSystem handler to file system
   */
  public FileSystem(alluxio.client.file.FileSystem fileSystem) {
    super(fileSystem);
  }

  @Override
  public String getScheme() {
    return Constants.SCHEME;
  }

  @Override
  protected boolean isZookeeperMode() {
    return mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
  }

  @Override
  protected AlluxioURI getAlluxioPath(Path path) {
    return new AlluxioURI(HadoopUtils.getPathWithoutScheme(path));
  }

  @Override
  protected Path getFsPath(String fsUriHeader, URIStatus fileStatus) {
    return new Path(fsUriHeader + fileStatus.getPath());
  }

  @Override
  protected void validateFsUri(URI fsUri) throws IOException, IllegalArgumentException {
    Preconditions.checkArgument(fsUri.getScheme().equals(getScheme()),
        PreconditionMessage.URI_SCHEME_MISMATCH.toString(), fsUri.getScheme(), getScheme());

    Authority auth = Authority.fromString(fsUri.getAuthority());
    if (auth instanceof UnknownAuthority) {
      throw new IOException(String.format("Authority \"%s\" is unknown. The client can not be "
          + "configured with the authority from %s", auth, fsUri));
    }
  }
}
