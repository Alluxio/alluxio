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

package alluxio.underfs.tos;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.volcengine.tos.TosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Factory for creating {@link TOSUnderFileSystem}.
 */
public class TOSUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TOSUnderFileSystemFactory.class);

  /**
   * Constructs a new {@link TOSUnderFileSystemFactory}.
   */
  public TOSUnderFileSystemFactory() {
  }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    if (checkTOSCredentials(conf)) {
      try {
        return TOSUnderFileSystem.createInstance(new AlluxioURI(path), conf);
      } catch (TosException e) {
        LOG.warn("Failed to create TOS Under File System: {}", e.getMessage());
        throw AlluxioTosException.from(e);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "TOS Credentials not available, cannot create TOS Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_TOS);
  }

  /**
   * @param conf optional configuration object for the UFS
   * @return true if both access, secret and endpoint keys are present, false otherwise
   */
  private boolean checkTOSCredentials(UnderFileSystemConfiguration conf) {
    return conf.isSet(PropertyKey.TOS_ACCESS_KEY)
        && conf.isSet(PropertyKey.TOS_SECRET_KEY)
        && conf.isSet(PropertyKey.TOS_ENDPOINT_KEY)
        && conf.isSet(PropertyKey.TOS_REGION);
  }
}
