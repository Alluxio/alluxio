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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;

import java.io.IOException;
import java.util.UUID;

/**
 * This class will use SoftLayer or OpenStack Swift as the backing store.
 * The integration properties should be specified in the module's pom file.
 * Each instance of the cluster will run with a separate base directory (user prefix + uuid).
 * Each test will attempt to clean up their test directories, but in cases of complete failure
 * (ie. jvm crashed) the directory will need to be cleaned up through manual means.
 */
public class SwiftUnderStorageCluster extends UnderFileSystemCluster {

  private static final String INTEGRATION_SWIFT_API_KEY = "apiKey";
  private static final String INTEGRATION_SWIFT_TENANT_KEY = "tenantKey";
  private static final String INTEGRATION_SWIFT_USER_KEY = "userKey";
  private static final String INTEGRATION_SWIFT_CONTAINER_KEY = "containerKey";
  private static final String INTEGRATION_SWIFT_AUTH_METHOD_KEY = "authMethodKey";
  private static final String INTEGRATION_SWIFT_AUTH_URL_KEY = "authUrlKey";

  private String mSwiftContainer;

  public SwiftUnderStorageCluster(String baseDir, Configuration configuration) {
    super(baseDir, configuration);
    String swiftAPIKey = System.getProperty(INTEGRATION_SWIFT_API_KEY);
    String tenantKey = System.getProperty(INTEGRATION_SWIFT_TENANT_KEY);
    String userKey = System.getProperty(INTEGRATION_SWIFT_USER_KEY);
    String authMethodKey = System.getProperty(INTEGRATION_SWIFT_AUTH_METHOD_KEY);
    String authUrlKey = System.getProperty(INTEGRATION_SWIFT_AUTH_URL_KEY);

    System.setProperty(Constants.SWIFT_API_KEY, swiftAPIKey);
    System.setProperty(Constants.SWIFT_TENANT_KEY, tenantKey);
    System.setProperty(Constants.SWIFT_USER_KEY, userKey);
    System.setProperty(Constants.SWIFT_AUTH_METHOD_KEY, authMethodKey);
    System.setProperty(Constants.SWIFT_AUTH_URL_KEY, authUrlKey);

    mSwiftContainer = System.getProperty(INTEGRATION_SWIFT_CONTAINER_KEY);
    mBaseDir = mSwiftContainer + UUID.randomUUID();
  }

  @Override
  public void cleanup() throws IOException {
    String oldDir = mBaseDir;
    mBaseDir = mSwiftContainer + UUID.randomUUID();
    UnderFileSystem ufs = UnderFileSystem.get(mBaseDir, mConfiguration);
    ufs.delete(oldDir, true);
  }

  @Override
  public String getUnderFilesystemAddress() {
    return mBaseDir;
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public void shutdown() throws IOException {}

  @Override
  public void start() throws IOException {}
}
