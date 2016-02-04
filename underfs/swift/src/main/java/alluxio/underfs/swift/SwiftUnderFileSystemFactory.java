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

package alluxio.underfs.swift;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;

/**
 * Factory for creating {@link SwiftUnderFileSystem}.
 */
@ThreadSafe
public class SwiftUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public UnderFileSystem create(String path, Configuration configuration, Object unusedConf) {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(configuration);

    if (addAndCheckSwiftCredentials(configuration)) {
      AlluxioURI uri = new AlluxioURI(path);
      try {
        return new SwiftUnderFileSystem(uri.getHost(), configuration);
      } catch (Exception e) {
        LOG.error("Failed to create SwiftUnderFileSystem.", e);
        throw Throwables.propagate(e);
      }
    }

    String err = "Swift Credentials not available, cannot create Swift Under File System.";
    LOG.error(err);
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path, Configuration configuration) {
    return path != null && path.startsWith(Constants.HEADER_SWIFT);
  }

  /**
   * Adds Swift credentials from system properties to the Tachyon configuration if they are not
   * already present.
   *
   * @param configuration the Tachyon configuration to check and add credentials to
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean addAndCheckSwiftCredentials(Configuration configuration) {
    String tenantApiKeyConf = Constants.SWIFT_API_KEY;
    if (System.getProperty(tenantApiKeyConf) != null
        || (configuration.containsKey(tenantApiKeyConf)
            && configuration.get(tenantApiKeyConf) == null)) {
      configuration.set(tenantApiKeyConf, System.getProperty(tenantApiKeyConf));
    }
    String tenantKeyConf = Constants.SWIFT_TENANT_KEY;
    if (System.getProperty(tenantKeyConf) != null
        || (configuration.containsKey(tenantKeyConf)
            && configuration.get(tenantKeyConf) == null)) {
      configuration.set(tenantKeyConf, System.getProperty(tenantKeyConf));
    }
    String tenantUserConf = Constants.SWIFT_USER_KEY;
    if (System.getProperty(tenantUserConf) != null
        || (configuration.containsKey(tenantUserConf)
            && configuration.get(tenantUserConf) == null)) {
      configuration.set(tenantUserConf, System.getProperty(tenantUserConf));
    }
    String tenantAuthURLKeyConf = Constants.SWIFT_AUTH_URL_KEY;
    if (System.getProperty(tenantAuthURLKeyConf) != null
        || (configuration.containsKey(tenantAuthURLKeyConf)
            && configuration.get(tenantAuthURLKeyConf) == null)) {
      configuration.set(tenantAuthURLKeyConf, System.getProperty(tenantAuthURLKeyConf));
    }
    String authMethodKeyConf = Constants.SWIFT_AUTH_METHOD_KEY;
    if (System.getProperty(authMethodKeyConf) != null
        || (configuration.containsKey(authMethodKeyConf)
            && configuration.get(authMethodKeyConf) == null)) {
      configuration.set(authMethodKeyConf, System.getProperty(authMethodKeyConf));
    }

    return configuration.get(tenantApiKeyConf) != null
        && configuration.get(tenantKeyConf) != null
        && configuration.get(tenantAuthURLKeyConf) != null
        && configuration.get(tenantUserConf) != null;
  }
}
