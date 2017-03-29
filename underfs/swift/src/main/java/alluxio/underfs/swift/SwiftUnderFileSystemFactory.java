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
import alluxio.PropertyKey;
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
  private static final Logger LOG = LoggerFactory.getLogger(SwiftUnderFileSystemFactory.class);

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
   * @return true if simulation mode or if all required authentication credentials are present
   */
  private boolean addAndCheckSwiftCredentials() {
    PropertyKey[] propertiesToRead = {PropertyKey.SWIFT_API_KEY, PropertyKey.SWIFT_TENANT_KEY,
        PropertyKey.SWIFT_USER_KEY, PropertyKey.SWIFT_AUTH_URL_KEY,
        PropertyKey.SWIFT_AUTH_METHOD_KEY, PropertyKey.SWIFT_PASSWORD_KEY,
        PropertyKey.SWIFT_SIMULATION, PropertyKey.SWIFT_REGION_KEY};

    for (PropertyKey property : propertiesToRead) {
      if (System.getProperty(property.toString()) != null
          && (!Configuration.containsKey(property) || Configuration.get(property) == null)) {
        Configuration.set(property, System.getProperty(property.toString()));
      }
    }

    // We do not need authentication credentials in simulation mode
    if (Configuration.containsKey(PropertyKey.SWIFT_SIMULATION)
        && Configuration.getBoolean(PropertyKey.SWIFT_SIMULATION)) {
      return true;
    }

    // API or Password Key is required
    PropertyKey apiOrPasswordKey = Configuration.containsKey(PropertyKey.SWIFT_API_KEY)
        ? PropertyKey.SWIFT_API_KEY : PropertyKey.SWIFT_PASSWORD_KEY;

    // Check if required credentials exist
    PropertyKey[] requiredProperties = {apiOrPasswordKey, PropertyKey.SWIFT_TENANT_KEY,
        PropertyKey.SWIFT_AUTH_URL_KEY, PropertyKey.SWIFT_USER_KEY};
    for (PropertyKey propertyName : requiredProperties) {
      if (Configuration.get(propertyName) == null) {
        return false;
      }
    }
    return true;
  }
}
