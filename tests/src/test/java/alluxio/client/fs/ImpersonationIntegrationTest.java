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
import alluxio.client.file.FileSystemClientOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.security.User;
import alluxio.security.group.GroupMappingService;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;

/**
 * Integration tests for user impersonation.
 */
public final class ImpersonationIntegrationTest extends BaseIntegrationTest {
  private static final String IMPERSONATION_USER = "impersonation_user";
  private static final String IMPERSONATION_GROUP1 = "impersonation_group1";
  private static final String IMPERSONATION_GROUP2 = "impersonation_group2";

  private static final String HDFS_USER = "hdfs_user";
  private static final String HDFS_GROUP1 = "hdfs_group1";
  private static final String HDFS_GROUP2 = "hdfs_group2";

  private static final String CONNECTION_USER = "alluxio_user";
  private static final String IMPERSONATION_GROUPS_CONFIG =
      "alluxio.master.security.impersonation.alluxio_user.groups";
  private static final String IMPERSONATION_USERS_CONFIG =
      "alluxio.master.security.impersonation.alluxio_user.users";
  private static final HashMap<String, String> GROUPS = new HashMap<>();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false)
          .setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, CONNECTION_USER)
          .setProperty(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS, 0)
          .setProperty(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
              CustomGroupMapping.class.getName()).build();

  @After
  public void after() throws Exception {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Before
  public void before() throws Exception {
    // Give the root dir 777, to write files as different users. This must be run as the user
    // that starts the master process
    FileSystem.Factory.get().setAttribute(new AlluxioURI("/"),
        FileSystemClientOptions.getSetAttributeOptions().toBuilder().setMode((short) 0777).build());
    // Enable client impersonation by default
    Configuration
        .set(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME, Constants.IMPERSONATION_HDFS_USER);
  }

  @BeforeClass
  public static void beforeClass() {
    GROUPS.put(IMPERSONATION_USER, IMPERSONATION_GROUP1 + "," + IMPERSONATION_GROUP2);
    GROUPS.put(HDFS_USER, HDFS_GROUP1 + "," + HDFS_GROUP2);
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {IMPERSONATION_GROUPS_CONFIG, "*"})
  public void impersonationNotUsed() throws Exception {
    Configuration
        .set(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME, Constants.IMPERSONATION_NONE);
    FileSystemContext context = FileSystemContext.get(createHdfsSubject());
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient(context);
    fs.createFile(new AlluxioURI("/impersonation-test")).close();
    List<URIStatus> listing = fs.listStatus(new AlluxioURI("/"));
    Assert.assertEquals(1, listing.size());
    URIStatus status = listing.get(0);
    Assert.assertNotEquals(IMPERSONATION_USER, status.getOwner());
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {IMPERSONATION_GROUPS_CONFIG, "*"})
  public void impersonationArbitraryUserDisallowed() throws Exception {
    String arbitraryUser = "arbitrary_user";
    Configuration
        .set(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME, arbitraryUser);
    FileSystemContext context = FileSystemContext.get(createHdfsSubject());
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient(context);
    fs.createFile(new AlluxioURI("/impersonation-test")).close();
    List<URIStatus> listing = fs.listStatus(new AlluxioURI("/"));
    Assert.assertEquals(1, listing.size());
    URIStatus status = listing.get(0);
    Assert.assertNotEquals(arbitraryUser, status.getOwner());
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {IMPERSONATION_GROUPS_CONFIG, "*"})
  public void impersonationUsedHdfsUser() throws Exception {
    // test using the hdfs subject
    checkCreateFile(createHdfsSubject(), HDFS_USER);
  }

  @Test
  public void impersonationHdfsDisabled() throws Exception {
    try {
      checkCreateFile(createHdfsSubject(), HDFS_USER);
      Assert.fail("Connection succeeded, but impersonation should be denied.");
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {IMPERSONATION_USERS_CONFIG, HDFS_USER})
  public void impersonationHdfsUserAllowed() throws Exception {
    checkCreateFile(createHdfsSubject(), HDFS_USER);
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {IMPERSONATION_USERS_CONFIG,
      "wrong_user1,wrong_user2," + HDFS_USER})
  public void impersonationHdfsUsersAllowed() throws Exception {
    checkCreateFile(createHdfsSubject(), HDFS_USER);
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {IMPERSONATION_USERS_CONFIG, "wrong_user"})
  public void impersonationHdfsUserDenied() throws Exception {
    try {
      checkCreateFile(createHdfsSubject(), HDFS_USER);
      Assert.fail("Connection succeeded, but impersonation should be denied.");
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {
          IMPERSONATION_USERS_CONFIG, HDFS_USER,
          IMPERSONATION_GROUPS_CONFIG, HDFS_GROUP1})
  public void impersonationUsersAllowedGroupsAllowed() throws Exception {
    checkCreateFile(createHdfsSubject(), HDFS_USER);
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {
          IMPERSONATION_USERS_CONFIG, "wrong_user",
          IMPERSONATION_GROUPS_CONFIG, HDFS_GROUP1})
  public void impersonationUsersDeniedGroupsAllowed() throws Exception {
    checkCreateFile(createHdfsSubject(), HDFS_USER);
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {
          IMPERSONATION_USERS_CONFIG, HDFS_USER,
          IMPERSONATION_GROUPS_CONFIG, "wrong_group"})
  public void impersonationUsersAllowedGroupsDenied() throws Exception {
    checkCreateFile(createHdfsSubject(), HDFS_USER);
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {
          IMPERSONATION_USERS_CONFIG, "wrong_user",
          IMPERSONATION_GROUPS_CONFIG, "wrong_group"})
  public void impersonationUsersDeniedGroupsDenied() throws Exception {
    try {
      checkCreateFile(createHdfsSubject(), HDFS_USER);
      Assert.fail("Connection succeeded, but impersonation should be denied.");
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {IMPERSONATION_GROUPS_CONFIG, HDFS_GROUP2})
  public void impersonationHdfsGroupAllowed() throws Exception {
    checkCreateFile(createHdfsSubject(), HDFS_USER);
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {IMPERSONATION_GROUPS_CONFIG,
      IMPERSONATION_GROUP1 + "," + IMPERSONATION_GROUP2 + "," + HDFS_GROUP1})
  public void impersonationHdfsGroupsAllowed() throws Exception {
    checkCreateFile(createHdfsSubject(), HDFS_USER);
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {IMPERSONATION_GROUPS_CONFIG, "wrong_group"})
  public void impersonationHdfsGroupDenied() throws Exception {
    try {
      checkCreateFile(createHdfsSubject(), HDFS_USER);
      Assert.fail("Connection succeeded, but impersonation should be denied.");
    } catch (IOException e) {
      // expected
    }
  }

  private void checkCreateFile(Subject subject, String expectedUser) throws Exception {
    FileSystemContext context = FileSystemContext.get(subject);
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient(context);
    fs.createFile(new AlluxioURI("/impersonation-test")).close();
    List<URIStatus> listing = fs.listStatus(new AlluxioURI("/"));
    Assert.assertEquals(1, listing.size());
    URIStatus status = listing.get(0);
    Assert.assertEquals(expectedUser, status.getOwner());
  }

  private Subject createHdfsSubject() {
    // Create a subject for an hdfs user
    User user = new User(HDFS_USER);
    Set<Principal> principals = new HashSet<>();
    principals.add(user);
    return new Subject(false, principals, new HashSet<>(), new HashSet<>());
  }

  public static class CustomGroupMapping implements GroupMappingService {
    public CustomGroupMapping() {
    }

    @Override
    public List<String> getGroups(String user) {
      if (GROUPS.containsKey(user)) {
        return Lists.newArrayList(GROUPS.get(user).split(","));
      }
      return new ArrayList<>();
    }
  }
}
