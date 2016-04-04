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

package alluxio.hadoop;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.security.authentication.AuthType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Integration tests for {@link FileSystem#setOwner(Path, String, String)} and
 * {@link FileSystem#setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)}.
 */
public final class FileSystemAclIntegrationTest {

  private static final int BLOCK_SIZE = 1024;
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(100000000, BLOCK_SIZE,
          Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName(),
          Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
  private static org.apache.hadoop.fs.FileSystem sTFS;

  private static void create(org.apache.hadoop.fs.FileSystem fs, Path path) throws IOException {
    FSDataOutputStream o = fs.create(path);
    o.writeBytes("Test Bytes");
    o.close();
  }

  public static void cleanup(org.apache.hadoop.fs.FileSystem fs) throws IOException {
    FileStatus[] statuses = fs.listStatus(new Path("/"));
    for (FileStatus f : statuses) {
      fs.delete(f.getPath(), true);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.alluxio.impl", FileSystem.class.getName());

    URI uri = URI.create(sLocalAlluxioClusterResource.get().getMasterUri());

    sTFS = org.apache.hadoop.fs.FileSystem.get(uri, conf);
  }

  @After
  public void cleanupTFS() throws Exception {
    cleanup(sTFS);
  }

  /**
   * Test for {@link FileSystem#setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)}.
   * It will test changing the permission of file using TFS.
   */
  @Test
  public void chmodTest() throws Exception {
    Path fileA = new Path("/chmodfileA");

    create(sTFS, fileA);
    FileStatus fs = sTFS.getFileStatus(fileA);
    // Default permission should be 0644
    Assert.assertEquals((short) 0644, fs.getPermission().toShort());

    sTFS.setPermission(fileA, FsPermission.createImmutable((short) 0755));
    Assert.assertEquals((short) 0755, sTFS.getFileStatus(fileA).getPermission().toShort());
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)}. It will test only changing the
   * owner of file using TFS.
   */
  @Test
  public void changeOwnerTest() throws Exception {
    Path fileA = new Path("/chownfileA");
    final String newOwner = "test-user1";
    final String newGroup = "test-group1";

    create(sTFS, fileA);

    FileStatus fs = sTFS.getFileStatus(fileA);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertNotEquals(defaultOwner, newOwner);
    Assert.assertNotEquals(defaultGroup, newGroup);

    sTFS.setOwner(fileA, newOwner, null);

    fs = sTFS.getFileStatus(fileA);
    Assert.assertEquals(newOwner, fs.getOwner());
    Assert.assertEquals(defaultGroup, fs.getGroup());
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)}. It will test only changing the
   * group of file using TFS.
   */
  @Test
  public void changeGroupTest() throws Exception {
    Path fileB = new Path("/chownfileB");
    final String newOwner = "test-user1";
    final String newGroup = "test-group1";

    create(sTFS, fileB);

    FileStatus fs = sTFS.getFileStatus(fileB);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertNotEquals(defaultOwner, newOwner);
    Assert.assertNotEquals(defaultGroup, newGroup);

    sTFS.setOwner(fileB, null, newGroup);

    fs = sTFS.getFileStatus(fileB);
    Assert.assertEquals(defaultOwner, fs.getOwner());
    Assert.assertEquals(newGroup, fs.getGroup());
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)}. It will test changing both owner
   * and group of file using TFS.
   */
  @Test
  public void changeOwnerAndGroupTest() throws Exception {
    Path fileC = new Path("/chownfileC");
    final String newOwner = "test-user1";
    final String newGroup = "test-group1";

    create(sTFS, fileC);

    FileStatus fs = sTFS.getFileStatus(fileC);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    Assert.assertNotEquals(defaultOwner, newOwner);
    Assert.assertNotEquals(defaultGroup, newGroup);

    sTFS.setOwner(fileC, newOwner, newGroup);

    fs = sTFS.getFileStatus(fileC);
    Assert.assertEquals(newOwner, fs.getOwner());
    Assert.assertEquals(newGroup, fs.getGroup());
  }

  /**
   * Test for {@link FileSystem#setOwner(Path, String, String)}. It will test both owner and group
   * are null.
   */
  @Test
  public void checkNullOwnerAndGroupTest() throws Exception {
    Path fileD = new Path("/chownfileD");

    create(sTFS, fileD);

    FileStatus fs = sTFS.getFileStatus(fileD);
    String defaultOwner = fs.getOwner();
    String defaultGroup = fs.getGroup();

    sTFS.setOwner(fileD, null, null);

    fs = sTFS.getFileStatus(fileD);
    Assert.assertEquals(defaultOwner, fs.getOwner());
    Assert.assertEquals(defaultGroup, fs.getGroup());
  }
}
