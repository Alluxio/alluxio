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
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.grpc.WritePType;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for setReplication command.
 */
public final class SetReplicationCommandTest extends AbstractFileSystemShellTest {
  private static final String TEST_FILE = "/testFile";

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Test
  public void setReplicationMin() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-min", "1", TEST_FILE);
    Assert.assertEquals(0, ret);

    URIStatus status = sFileSystem.getStatus(new AlluxioURI(TEST_FILE));
    Assert.assertEquals(1, status.getReplicationMin());
  }

  @Test
  public void setReplicationMax() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-max", "2", TEST_FILE);
    Assert.assertEquals(0, ret);

    URIStatus status = sFileSystem.getStatus(new AlluxioURI(TEST_FILE));
    Assert.assertEquals(2, status.getReplicationMax());
  }

  @Test
  public void setReplicationMinMax() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-min", "1", "-max", "2", TEST_FILE);
    Assert.assertEquals(0, ret);

    URIStatus status = sFileSystem.getStatus(new AlluxioURI(TEST_FILE));
    Assert.assertEquals(1, status.getReplicationMin());
    Assert.assertEquals(2, status.getReplicationMax());
  }

  @Test
  public void setReplicationNoMinMax() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", TEST_FILE);
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationBadMinMax() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-min", "2", "-max", "1", TEST_FILE);
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationBadMinMaxSeparately() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-min", "2", TEST_FILE);
    Assert.assertEquals(0, ret);
    ret = sFsShell.run("setReplication", "-max", "1", TEST_FILE);
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationZeroMin() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-min", "0", TEST_FILE);
    Assert.assertEquals(0, ret);

    URIStatus status = sFileSystem.getStatus(new AlluxioURI(TEST_FILE));
    Assert.assertEquals(0, status.getReplicationMin());
  }

  @Test
  public void setReplicationNegativeMax() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-max", "-2", TEST_FILE);
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationInfinityMax() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-max", "-1", TEST_FILE);
    Assert.assertEquals(0, ret);

    URIStatus status = sFileSystem.getStatus(new AlluxioURI(TEST_FILE));
    Assert.assertEquals(-1, status.getReplicationMax());
  }

  @Test
  public void setReplicationZeroMax() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-max", "0", TEST_FILE);
    Assert.assertEquals(0, ret);

    URIStatus status = sFileSystem.getStatus(new AlluxioURI(TEST_FILE));
    Assert.assertEquals(0, status.getReplicationMin());
  }

  @Test
  public void setReplicationNegativeMin() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-min", "-2", TEST_FILE);
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationNegativeMinMax() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, TEST_FILE, WritePType.MUST_CACHE, 10);
    int ret = sFsShell.run("setReplication", "-min", "-2", "-max", "-1", TEST_FILE);
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void setReplicationRecursively() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    int ret =
        sFsShell.run("setReplication", "-R", "-min", "2", PathUtils.concatPath(testDir, "foo"));
    Assert.assertEquals(0, ret);

    URIStatus status1 =
        sFileSystem.getStatus(new AlluxioURI(PathUtils.concatPath(testDir, "foo", "foobar1")));
    URIStatus status2 =
        sFileSystem.getStatus(new AlluxioURI(PathUtils.concatPath(testDir, "foo", "foobar2")));
    Assert.assertEquals(2, status1.getReplicationMin());
    Assert.assertEquals(2, status2.getReplicationMin());
  }
}
