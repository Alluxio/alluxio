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

package alluxio.underfs.oss;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link OSSUnderFileSystem}.
 */
@ThreadSafe
public class OSSUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link OSSUnderFileSystemFactory}.
   */
  public OSSUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, Object ufsConf) {
    Preconditions.checkNotNull(path);

    if (checkOSSCredentials()) {
      try {
        return OSSUnderFileSystem.createInstance(new AlluxioURI(path));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "OSS Credentials not available, cannot create OSS Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_OSS);
  }

  /**
   * @return true if both access, secret and endpoint keys are present, false otherwise
   */
  private boolean checkOSSCredentials() {
    return Configuration.containsKey(PropertyKey.OSS_ACCESS_KEY)
        && Configuration.containsKey(PropertyKey.OSS_SECRET_KEY)
        && Configuration.containsKey(PropertyKey.OSS_ENDPOINT_KEY);
  }
}
