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
package alluxio.security;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;

import alluxio.exception.ExceptionMessage;
import alluxio.master.MasterTestUtils;
import alluxio.master.file.FileSystemMaster;

import alluxio.security.authentication.AuthenticatedClientUser;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for starting a cluster when security is enabled.
 */
public class ClusterInitializationTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private static final String SUPER_USER = "alluxio";
  private static final String USER = "jack";

  private static final AlluxioURI ROOT = new AlluxioURI("/");

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(
          LocalAlluxioClusterResource.DEFAULT_WORKER_CAPACITY_BYTES,
          LocalAlluxioClusterResource.DEFAULT_USER_BLOCK_SIZE,
          Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
          Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");

  /**
   * When a user starts a new cluster, an empty root dir is created and owned by the user.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {Constants.SECURITY_LOGIN_USERNAME, SUPER_USER})
  public void startClusterTest() throws Exception {
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    URIStatus status = fs.getStatus(ROOT);
    Assert.assertEquals(SUPER_USER, status.getUserName());
    Assert.assertEquals(0755, status.getPermission());

    Assert.assertEquals(0, fs.listStatus(new AlluxioURI("/")).size());
  }

  /**
   * When a user starts a cluster with journal logs, which are generated by previous running
   * cluster owned by the same user, it should succeed.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {Constants.SECURITY_LOGIN_USERNAME, SUPER_USER})
  public void recoverClusterSuccessTest() throws Exception {
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    fs.createFile(new AlluxioURI("/testFile"));
    mLocalAlluxioClusterResource.get().stopFS();

    Configuration conf = mLocalAlluxioClusterResource.get().getMasterConf();
    LoginUserTestUtils.resetLoginUser(conf, SUPER_USER);

    // user alluxio can recover master from journal
    FileSystemMaster fileSystemMaster = MasterTestUtils.createFileSystemMasterFromJournal(conf);

    AuthenticatedClientUser.set(SUPER_USER);
    Assert.assertEquals(SUPER_USER,
        fileSystemMaster.getFileInfo(new AlluxioURI("/testFile")).getUserName());
  }

  /**
   * When a user starts a cluster with journal logs, which are generated by previous running
   * cluster owned by a different user, it should fail and throw an exception.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {Constants.SECURITY_LOGIN_USERNAME, SUPER_USER})
  public void recoverClusterFailTest() throws Exception {
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(ExceptionMessage.PERMISSION_DENIED
        .getMessage("Unauthorized user on root"));

    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    fs.createFile(new AlluxioURI("/testFile"));
    mLocalAlluxioClusterResource.get().stopFS();

    Configuration conf = mLocalAlluxioClusterResource.get().getMasterConf();
    LoginUserTestUtils.resetLoginUser(conf, USER);

    // user jack cannot recover master from journal, in which the root is owned by alluxio.
    MasterTestUtils.createFileSystemMasterFromJournal(conf);
  }
}
