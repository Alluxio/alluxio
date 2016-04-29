/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link OSSUnderFileSystem}.
 */
@ThreadSafe
public class OSSUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public UnderFileSystem create(String path, Configuration configuration, Object ufsConf) {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(configuration);

    if (addAndCheckOSSCredentials(configuration)) {
      try {
        return new OSSUnderFileSystem(new AlluxioURI(path), configuration);
      } catch (Exception e) {
        LOG.error("Failed to create OSSUnderFileSystem.", e);
        throw Throwables.propagate(e);
      }
    }

    String err = "OSS Credentials not available, cannot create OSS Under File System.";
    LOG.error(err);
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path, Configuration configuration) {
    return path != null && path.startsWith(Constants.HEADER_OSS);
  }

  /**
   * Adds OSS credentials from system properties to the Alluxio configuration if they are not
   * already present.
   *
   * @param configuration the Alluxio configuration to check and add credentials to
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean addAndCheckOSSCredentials(Configuration configuration) {
    String accessKeyConf = Constants.OSS_ACCESS_KEY;
    if (System.getProperty(accessKeyConf) != null && configuration.get(accessKeyConf) == null) {
      configuration.set(accessKeyConf, System.getProperty(accessKeyConf));
    }
    String secretKeyConf = Constants.OSS_SECRET_KEY;
    if (System.getProperty(secretKeyConf) != null && configuration.get(secretKeyConf) == null) {
      configuration.set(secretKeyConf, System.getProperty(secretKeyConf));
    }
    String endPointConf = Constants.OSS_ENDPOINT_KEY;
    if (System.getProperty(endPointConf) != null && configuration.get(endPointConf) == null) {
      configuration.set(endPointConf, System.getProperty(endPointConf));
    }
    return configuration.get(accessKeyConf) != null
        && configuration.get(secretKeyConf) != null
        && configuration.get(endPointConf) != null;
  }
}
