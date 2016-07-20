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

  /**
   * Constructs a new {@link SwiftUnderFileSystemFactory}.
   */
  public SwiftUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, Object unusedConf) {
    Preconditions.checkNotNull(path);

    if (addAndCheckSwiftCredentials()) {
      try {
        return new SwiftUnderFileSystem(new AlluxioURI(path));
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
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_SWIFT);
  }

  /**
   * Adds Swift credentials from system properties to the Alluxio configuration if they are not
   * already present.
   *
   * @return true if simulation mode or if both access and secret key are present, false otherwise
   */
  private boolean addAndCheckSwiftCredentials() {
    String tenantApiKeyConf = Constants.SWIFT_API_KEY;
    if (System.getProperty(tenantApiKeyConf) != null && (
        !Configuration.containsKey(tenantApiKeyConf)
            || Configuration.get(tenantApiKeyConf) == null)) {
      Configuration.set(tenantApiKeyConf, System.getProperty(tenantApiKeyConf));
    }
    String tenantKeyConf = Constants.SWIFT_TENANT_KEY;
    if (System.getProperty(tenantKeyConf) != null && (!Configuration.containsKey(tenantKeyConf)
        || Configuration.get(tenantKeyConf) == null)) {
      Configuration.set(tenantKeyConf, System.getProperty(tenantKeyConf));
    }
    String tenantUserConf = Constants.SWIFT_USER_KEY;
    if (System.getProperty(tenantUserConf) != null && (!Configuration.containsKey(tenantUserConf)
        || Configuration.get(tenantUserConf) == null)) {
      Configuration.set(tenantUserConf, System.getProperty(tenantUserConf));
    }
    String tenantAuthURLKeyConf = Constants.SWIFT_AUTH_URL_KEY;
    if (System.getProperty(tenantAuthURLKeyConf) != null && (
        !Configuration.containsKey(tenantAuthURLKeyConf)
            || Configuration.get(tenantAuthURLKeyConf) == null)) {
      Configuration.set(tenantAuthURLKeyConf, System.getProperty(tenantAuthURLKeyConf));
    }
    String authMethodKeyConf = Constants.SWIFT_AUTH_METHOD_KEY;
    if (System.getProperty(authMethodKeyConf) != null && (
        !Configuration.containsKey(authMethodKeyConf)
            || Configuration.get(authMethodKeyConf) == null)) {
      Configuration.set(authMethodKeyConf, System.getProperty(authMethodKeyConf));
    }
    String passwordKeyConf = Constants.SWIFT_PASSWORD_KEY;
    if (System.getProperty(passwordKeyConf) != null && (!Configuration.containsKey(passwordKeyConf)
        || Configuration.get(passwordKeyConf) == null)) {
      Configuration.set(passwordKeyConf, System.getProperty(passwordKeyConf));
    }

    String swiftSimulationConf = Constants.SWIFT_SIMULATION;
    if (System.getProperty(swiftSimulationConf) != null && (
         !Configuration.containsKey(swiftSimulationConf)
             || Configuration.get(swiftSimulationConf) == null)) {
      Configuration.set(swiftSimulationConf, System.getProperty(swiftSimulationConf));
    }

    boolean simulation = false;
    if (Configuration.containsKey(swiftSimulationConf)) {
      simulation = Configuration.getBoolean(swiftSimulationConf);
    }

    // We do not need authentication credentials in simulation mode
    return simulation || (((Configuration.containsKey(tenantApiKeyConf)
        && Configuration.get(tenantApiKeyConf) != null) || (
        Configuration.containsKey(passwordKeyConf) && Configuration.get(passwordKeyConf) != null))
        && Configuration.get(tenantKeyConf) != null
        && Configuration.get(tenantAuthURLKeyConf) != null
        && Configuration.get(tenantUserConf) != null);
  }
}
