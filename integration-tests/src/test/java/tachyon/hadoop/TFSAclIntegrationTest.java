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

package tachyon.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.security.authentication.AuthType;

/**
 * Integration tests for {@link TFS#setOwner(Path, String, String)} and
 * {@link TFS#setPermission(Path, org.apache.hadoop.fs.permission.FsPermission)}.
 */
public class TFSAclIntegrationTest {

  private static final int BLOCK_SIZE = 1024;
  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(100000000, 100000, BLOCK_SIZE,
          Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName(),
          Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true");
  private static FileSystem sTFS;

  private static void create(FileSystem fs, Path path) throws IOException {
    FSDataOutputStream o = fs.create(path);
    o.writeBytes("Test Bytes");
    o.close();
  }

  public static void cleanup(FileSystem fs) throws IOException {
    FileStatus[] statuses = fs.listStatus(new Path("/"));
    for (FileStatus f : statuses) {
      fs.delete(f.getPath(), true);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.tachyon.impl", TFS.class.getName());

    URI uri = URI.create(sLocalTachyonClusterResource.get().getMasterUri());

    sTFS = FileSystem.get(uri, conf);
  }

  @Test
  public void chmodTest() throws Exception {
    Path fileA = new Path("/chmodfileA");

    create(sTFS, fileA);
    FileStatus fs = sTFS.getFileStatus(fileA);
    short permission = fs.getPermission().toShort();
    // Default permission should be 0644
    Assert.assertEquals((short) 0644, permission);

    sTFS.setPermission(fileA, FsPermission.createImmutable((short) 0755));
    Assert.assertEquals((short) 0755, sTFS.getFileStatus(fileA).getPermission().toShort());

    cleanup(sTFS);

  }

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

    cleanup(sTFS);
  }

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

    cleanup(sTFS);
  }

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

    cleanup(sTFS);
  }
}
