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

package tachyon.underfs.swift;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemFactory;

/**
 * Factory for creating {@link SwiftUnderFileSystem}.
 */
public class SwiftUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public UnderFileSystem create(String path, TachyonConf tachyonConf, Object unusedConf) {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(tachyonConf);

    if (addAndCheckSwiftCredentials(tachyonConf)) {
      TachyonURI uri = new TachyonURI(path);
      try {
        return new SwiftUnderFileSystem(uri.getHost(), tachyonConf);
      } catch (Exception se) {
        LOG.error("Failed to create SwiftUnderFileSystem.", se);
        throw Throwables.propagate(se);
      }
    }

    String err = "Swift Credentials not available, cannot create Swift Under File System.";
    LOG.error(err);
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path, TachyonConf tachyonConf) {
    return path != null && path.startsWith(Constants.HEADER_SWIFT);
  }

  /**
   * Adds Swift credentials from system properties to the Tachyon configuration if they are not
   * already present.
   *
   * @param tachyonConf the Tachyon configuration to check and add credentials to
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean addAndCheckSwiftCredentials(TachyonConf tachyonConf) {
    String tenantApiKeyConf = Constants.SWIFT_API_KEY;
    if (System.getProperty(tenantApiKeyConf) != null
        || (tachyonConf.containsKey(tenantApiKeyConf)
            && tachyonConf.get(tenantApiKeyConf) == null)) {
      tachyonConf.set(tenantApiKeyConf, System.getProperty(tenantApiKeyConf));
    }
    String tenantKeyConf = Constants.SWIFT_TENANT_KEY;
    if (System.getProperty(tenantKeyConf) != null
        || (tachyonConf.containsKey(tenantKeyConf)
            && tachyonConf.get(tenantKeyConf) == null)) {
      tachyonConf.set(tenantKeyConf, System.getProperty(tenantKeyConf));
    }
    String tenantUserConf = Constants.SWIFT_USER_KEY;
    if (System.getProperty(tenantUserConf) != null
        || (tachyonConf.containsKey(tenantUserConf)
            && tachyonConf.get(tenantUserConf) == null)) {
      tachyonConf.set(tenantUserConf, System.getProperty(tenantUserConf));
    }
    String tenantAuthURLKeyConf = Constants.SWIFT_AUTH_URL_KEY;
    if (System.getProperty(tenantAuthURLKeyConf) != null
        || (tachyonConf.containsKey(tenantAuthURLKeyConf)
            && tachyonConf.get(tenantAuthURLKeyConf) == null)) {
      tachyonConf.set(tenantAuthURLKeyConf, System.getProperty(tenantAuthURLKeyConf));
    }
    String authMethodKeyConf = Constants.SWIFT_AUTH_METHOD_KEY;
    if (System.getProperty(authMethodKeyConf) != null
        || (tachyonConf.containsKey(authMethodKeyConf)
            && tachyonConf.get(authMethodKeyConf) == null)) {
      tachyonConf.set(authMethodKeyConf, System.getProperty(authMethodKeyConf));
    }

    return tachyonConf.get(tenantApiKeyConf) != null
        && tachyonConf.get(tenantKeyConf) != null
        && tachyonConf.get(tenantAuthURLKeyConf) != null
        && tachyonConf.get(tenantUserConf) != null;
  }
}
