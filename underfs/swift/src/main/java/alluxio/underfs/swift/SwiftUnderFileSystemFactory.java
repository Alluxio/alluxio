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

package alluxio.underfs.swift;

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
      try {
        return new SwiftUnderFileSystem(new AlluxioURI(path), configuration);
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
   * Adds Swift credentials from system properties to the Alluxio configuration if they are not
   * already present.
   *
   * @param configuration the Alluxio configuration to check and add credentials to
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
