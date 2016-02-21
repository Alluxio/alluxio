/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
      AlluxioURI uri = new AlluxioURI(path);
      try {
        return new OSSUnderFileSystem(uri.getHost(), configuration);
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
