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

package alluxio.underfs.swift;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link SwiftUnderFileSystem}.
 */
@ThreadSafe
public class SwiftUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SwiftUnderFileSystemFactory.class);

  /**
   * Constructs a new {@link SwiftUnderFileSystemFactory}.
   */
  public SwiftUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    if (checkSwiftCredentials(conf)) {
      try {
        return new SwiftUnderFileSystem(new AlluxioURI(path), conf);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "Swift Credentials not available, cannot create Swift Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_SWIFT);
  }

  /**
   * @param conf optional configuration object for the UFS
   *
   * @return true if simulation mode or if all required authentication credentials are present
   */
  private boolean checkSwiftCredentials(UnderFileSystemConfiguration conf) {
    // We do not need authentication credentials in simulation mode
    if (conf.isSet(PropertyKey.SWIFT_SIMULATION)
        && Boolean.valueOf(conf.get(PropertyKey.SWIFT_SIMULATION))) {
      return true;
    }

    // Check if required credentials exist
    return conf.isSet(PropertyKey.SWIFT_PASSWORD_KEY)
        && conf.isSet(PropertyKey.SWIFT_TENANT_KEY)
        && conf.isSet(PropertyKey.SWIFT_AUTH_URL_KEY)
        && conf.isSet(PropertyKey.SWIFT_USER_KEY);
  }
}
