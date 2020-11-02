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

package alluxio.underfs.bos;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link BOSUnderFileSystem}.
 */
@ThreadSafe
public class BOSUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link BOSUnderFileSystemFactory}.
   */
  public BOSUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    if (checkBOSCredentials(conf)) {
      try {
        return BOSUnderFileSystem.createInstance(new AlluxioURI(path), conf);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "BOS Credentials not available, cannot create BOS Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_BOS);
  }

  /**
   * @param conf optional configuration object for the UFS
   *
   * @return true if both access, secret and endpoint keys are present, false otherwise
   */
  private boolean checkBOSCredentials(UnderFileSystemConfiguration conf) {
    return conf.isSet(PropertyKey.BOS_ACCESS_KEY)
        && conf.isSet(PropertyKey.BOS_SECRET_KEY)
        && conf.isSet(PropertyKey.BOS_ENDPOINT_KEY);
  }
}
