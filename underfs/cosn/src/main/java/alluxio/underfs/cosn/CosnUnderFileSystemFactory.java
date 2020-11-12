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

package alluxio.underfs.cosn;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.conf.PropertyKey;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link CosnUnderFileSystem}.
 * It will ensure AWS credentials are present before
 * returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public class CosnUnderFileSystemFactory implements UnderFileSystemFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CosnUnderFileSystemFactory.class);

  /**
   * Constructs a new {@link CosnUnderFileSystemFactory}.
   */
  public CosnUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkArgument(path != null, "path may not be null");
    if (checkCosnCredentials(conf)) {
      return CosnUnderFileSystem.createInstance(new AlluxioURI(path), conf);
    }

    String err = "Credentials not available, cannot create COSN Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_COSN);
  }

  @Override
  public boolean supportsPath(String path, UnderFileSystemConfiguration conf) {
    return supportsPath(path);
  }

  /**
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean checkCosnCredentials(UnderFileSystemConfiguration conf) {
    return conf.isSet(PropertyKey.COSN_ACCESS_KEY)
        && conf.isSet(PropertyKey.COSN_SECRET_KEY)
        && conf.isSet(PropertyKey.COSN_REGION);
  }
}
