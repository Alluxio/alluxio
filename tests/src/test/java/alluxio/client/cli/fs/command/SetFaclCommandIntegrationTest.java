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

package alluxio.client.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;
import alluxio.security.user.TestUserState;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for setfacl command.
 */
@LocalAlluxioClusterResource.ServerConfig(
    confParams = {
        PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
        PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
        PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS,
        "alluxio.security.group.provider.IdentityUserGroupsMapping",
        PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP, "setfacl_test_user"})
public final class SetFaclCommandIntegrationTest extends AbstractFileSystemShellTest {
  private static final List<String> FACL_STRING_ENTRIES
      = Arrays.asList("user::rw-", "group::r--", "other::r--");
  private static final List<String> DIR_FACL_STRING_ENTRIES
      = Arrays.asList("user::rwx", "group::r-x", "other::r-x");
  private static final List<String> FILE_FACL_STRING_ENTRIES
      = Arrays.asList("user::rw-", "group::r-x", "other::r--");
  private static final List<String> DEFAULT_FACL_STRING_ENTRIES
      = Arrays.asList("default:user::rwx", "default:group::r-x", "default:other::r-x");

  /**
   * Tests setfacl command.
   */
  @Test
  public void setfacl() throws Exception {
    String testOwner = "setfacl_test_user";
    String expected = "";
    URIStatus[] files = createFiles(testOwner);

    sFsShell.run("setfacl", "-m", "user:testuser:rwx", "/testRoot/testFileA");
    sFsShell.run("getfacl", "/testRoot/testFileA");

    List<String> stringEntries = new ArrayList<>(FACL_STRING_ENTRIES);
    stringEntries.add("user:testuser:rwx");
    stringEntries.add("mask::rwx");
    expected += getFaclResultStr(testOwner, testOwner, "/testRoot/testFileA", stringEntries);

    Assert.assertEquals(expected, mOutput.toString());

    sFsShell.run("setfacl", "-m", "user::rwx", "/testRoot/testFileC");
    sFsShell.run("getfacl", "/testRoot/testFileC");

    stringEntries = new ArrayList<>(FACL_STRING_ENTRIES);
    stringEntries.set(0, "user::rwx");
    expected += getFaclResultStr(testOwner, testOwner, "/testRoot/testFileC", stringEntries);
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests setfacl command to set default facl.
   */
  @Test
  public void setDefaultFacl() throws Exception {
    String testOwner = "setfacl_test_user";
    URIStatus[] files = createFiles(testOwner);
    sFsShell.run("setfacl", "-m", "default:user:testuser:rwx", "/testRoot/testDir");
    sFsShell.run("getfacl", "/testRoot/testDir");

    List<String> stringEntries = new ArrayList<>(DIR_FACL_STRING_ENTRIES);
    stringEntries.addAll(DEFAULT_FACL_STRING_ENTRIES);
    stringEntries.add("default:user:testuser:rwx");
    stringEntries.add("default:mask::rwx");
    String expected = getFaclResultStr(testOwner, testOwner, "/testRoot/testDir", stringEntries);

    Assert.assertEquals(expected, mOutput.toString());

    FileSystemTestUtils.createByteFile(sFileSystem,
        "/testRoot/testDir/testDir2/testFileD", WritePType.MUST_CACHE, 10);

    sFsShell.run("getfacl", "/testRoot/testDir/testDir2");
    stringEntries = new ArrayList<>(DIR_FACL_STRING_ENTRIES);
    stringEntries.add("user:testuser:rwx");
    stringEntries.add("mask::r-x");
    stringEntries.addAll(DEFAULT_FACL_STRING_ENTRIES);
    stringEntries.add("default:user:testuser:rwx");
    stringEntries.add("default:mask::rwx");
    expected += getFaclResultStr(testOwner, testOwner,
        "/testRoot/testDir/testDir2", stringEntries);

    Assert.assertEquals(expected, mOutput.toString());

    sFsShell.run("getfacl", "/testRoot/testDir/testDir2/testFileD");
    stringEntries = new ArrayList<>(FILE_FACL_STRING_ENTRIES);
    stringEntries.add("user:testuser:rwx");
    stringEntries.add("mask::r--");
    expected += getFaclResultStr(testOwner, testOwner,
        "/testRoot/testDir/testDir2/testFileD", stringEntries);
    Assert.assertEquals(expected, mOutput.toString());
  }

  private String getFaclResultStr(String testUser, String testGroup,
      String fileName, List<String> perms) {
    StringBuilder sb = new StringBuilder();
    sb.append("# file: ");
    sb.append(fileName);
    sb.append("\n# owner: ");
    sb.append(testUser);
    sb.append("\n# group: ");
    sb.append(testGroup);
    sb.append("\n");

    for (String perm: perms) {
      sb.append(perm);
      sb.append("\n");
    }
    return sb.toString();
  }

  // Helper function to create a set of files in the file system
  private URIStatus[] createFiles(String user) throws IOException, AlluxioException {
    FileSystem fs = sFileSystem;
    if (user != null) {
      fs = sLocalAlluxioCluster.getClient(FileSystemContext
          .create(new TestUserState(user, ServerConfiguration.global()).getSubject(),
              ServerConfiguration.global()));
    }

    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileA",
        WritePType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testDir/testFileB",
        WritePType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(fs, "/testRoot/testFileC", WritePType.THROUGH,
        30);

    URIStatus[] files = new URIStatus[4];
    files[0] = fs.getStatus(new AlluxioURI("/testRoot/testFileA"));
    files[1] = fs.getStatus(new AlluxioURI("/testRoot/testDir"));
    files[2] = fs.getStatus(new AlluxioURI("/testRoot/testDir/testFileB"));
    files[3] = fs.getStatus(new AlluxioURI("/testRoot/testFileC"));
    return files;
  }
}
