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

package alluxio.underfs.cos;

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
 * Factory for creating {@link COSUnderFileSystem}.
 */
@ThreadSafe
public class COSUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link COSUnderFileSystemFactory}.
   */
  public COSUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    if (checkCOSCredentials(conf)) {
      try {
        return COSUnderFileSystem.createInstance(new AlluxioURI(path), conf);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "COS Credentials not available, cannot create COS Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_COS);
  }

  /**
   * @param conf optional configuration object for the UFS
   *
   * @return true if both access, secret and endpoint keys are present, false otherwise
   */
  private boolean checkCOSCredentials(UnderFileSystemConfiguration conf) {
    return conf.isSet(PropertyKey.COS_ACCESS_KEY)
        && conf.isSet(PropertyKey.COS_SECRET_KEY)
        && conf.isSet(PropertyKey.COS_REGION);
  }
}
