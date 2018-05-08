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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.security.User;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;

/**
 * Integration tests for user impersonation.
 */
public final class ImpersonationIntegrationTest extends BaseIntegrationTest {
  private static final String IMPERSONATION_USER = "impersonation-user";
  private static final String HDFS_USER = "hdfs-user";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Before
  public void before() throws Exception {
    // Give the root dir 777, to write files as different users.
    FileSystem.Factory.get().setAttribute(new AlluxioURI("/"),
        SetAttributeOptions.defaults().setMode(new Mode((short) 0777)));
  }

  @Test
  public void impersonationOn() throws Exception {
    Configuration.set(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME, IMPERSONATION_USER);
    FileSystemContext context = FileSystemContext.create(null);
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient(context);
    fs.createFile(new AlluxioURI("/impersonation-on")).close();
    List<URIStatus> listing = fs.listStatus(new AlluxioURI("/"));
    Assert.assertTrue(listing.size() == 1);
    URIStatus status = listing.get(0);
    Assert.assertEquals(IMPERSONATION_USER, status.getOwner());
  }

  @Test
  public void impersonationOff() throws Exception {
    Configuration.set(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME, "");
    FileSystemContext context = FileSystemContext.create(null);
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient(context);
    fs.createFile(new AlluxioURI("/impersonation-off")).close();
    List<URIStatus> listing = fs.listStatus(new AlluxioURI("/"));
    Assert.assertTrue(listing.size() == 1);
    URIStatus status = listing.get(0);
    Assert.assertNotEquals(IMPERSONATION_USER, status.getOwner());
  }

  @Test
  public void impersonationHdfsUser() throws Exception {
    Configuration
        .set(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME, Constants.IMPERSONATION_HDFS_USER);

    // Create a subject for an hdfs user
    User user = new User(HDFS_USER);
    Set<Principal> principals = new HashSet<>();
    principals.add(user);
    Subject subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());

    // Create a context using the hdfs subject
    FileSystemContext context = FileSystemContext.create(subject);
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient(context);
    fs.createFile(new AlluxioURI("/impersonation-hdfs")).close();
    List<URIStatus> listing = fs.listStatus(new AlluxioURI("/"));
    Assert.assertTrue(listing.size() == 1);
    URIStatus status = listing.get(0);
    Assert.assertEquals(HDFS_USER, status.getOwner());
  }
}
