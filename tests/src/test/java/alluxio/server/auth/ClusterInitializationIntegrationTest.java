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

package alluxio.server.auth;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for starting a cluster when security is enabled.
 */
public final class ClusterInitializationIntegrationTest extends BaseIntegrationTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private static final String SUPER_USER = "alluxio";

  private static final AlluxioURI ROOT = new AlluxioURI("/");

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build()
      .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.name())
      .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");

  /**
   * When a user starts a new cluster, an empty root dir is created and owned by the user.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.SECURITY_LOGIN_USERNAME, SUPER_USER})
  public void startCluster() throws Exception {
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    URIStatus status = fs.getStatus(ROOT);
    assertEquals(SUPER_USER, status.getOwner());
    assertEquals(0755, status.getMode());

    assertEquals(0, fs.listStatus(new AlluxioURI("/")).size());
  }
}
