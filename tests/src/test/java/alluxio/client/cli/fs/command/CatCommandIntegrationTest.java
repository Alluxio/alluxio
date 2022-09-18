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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import alluxio.AlluxioURI;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.WritePType;
import alluxio.util.io.BufferUtils;

import java.io.File;
import java.io.PrintWriter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for cat command.
 */
public final class CatCommandIntegrationTest extends AbstractFileSystemShellTest {

  @Test
  public void catAfterForceMasterSync() throws Exception {
    // Create a file in the UFS and write some bytes into it
    String testFilePath = "/testPersist/testFile";
    int size = 100;
    FileSystemTestUtils.createByteFile(sFileSystem, testFilePath, WritePType.THROUGH, size);
    assertTrue(sFileSystem.getStatus(new AlluxioURI("/testPersist/testFile")).isPersisted());

    // Update the file with a smaller size in the UFS directly
    String rootUfsDir = sLocalAlluxioCluster.getClient().getConf()
        .get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS).toString();
    File ufsFile = new File(rootUfsDir + testFilePath);
    ufsFile.delete();
    PrintWriter out = new PrintWriter(ufsFile);
    String newContent = "t";
    out.print(newContent);
    out.close();

    // Get the status of the file in Alluxio, the status should not be changed
    AlluxioURI uri = new AlluxioURI(testFilePath);
    URIStatus uriStatus = sFileSystem.getStatus(uri);
    long length = uriStatus.getLength();
    assertEquals(size, length);

    // Read the file by executing "alluxio fs cat <FILE_PATH>".
    // Exception should be thrown when the worker try reading bytes and returned value is -1 here
    // After that the master is forced to sync
    int ret = sFsShell.run("cat", testFilePath);
    assertEquals(-1, ret);
    assertTrue(mOutput.toString()
        .contains("Please ensure its metadata is consistent between Alluxio and UFS."));

    // Now the metadata should be consistent, and we can the file's size is updated.
    URIStatus updatedUriStatus = sFileSystem.getStatus(new AlluxioURI(testFilePath));
    long updatedLength = updatedUriStatus.getLength();
    assertEquals(newContent.getBytes().length, updatedLength);

    // The updated value should be read out here.
    try (FileInStream tfis = sFileSystem.openFile(uri)) {
      byte[] actual = new byte[newContent.getBytes().length];
      tfis.read(actual);
      assertArrayEquals(newContent.getBytes(), actual);
    }
    ret = sFsShell.run("cat", testFilePath);
    assertEquals(0, ret);
  }

  @Test
  public void catDirectory() throws Exception {
    String[] command = new String[] {"mkdir", "/testDir"};
    sFsShell.run(command);
    int ret = sFsShell.run("cat", "/testDir");
    Assert.assertEquals(-1, ret);
    String expected = getCommandOutput(command);
    expected += "Path \"/testDir\" must be a file.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  @Test
  public void catNotExist() throws Exception {
    int ret = sFsShell.run("cat", "/testFile");
    Assert.assertEquals(-1, ret);
  }

  @Test
  public void cat() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE, 10);
    sFsShell.run("cat", "/testFile");
    byte[] expect = BufferUtils.getIncreasingByteArray(10);
    Assert.assertArrayEquals(expect, mOutput.toByteArray());
  }

  @Test
  public void catWildcard() throws Exception {
    String testDir = FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem);
    // the expect contents (remember that the order is based on path)
    byte[] exp1 = BufferUtils.getIncreasingByteArray(30); // testDir/bar/foobar3
    byte[] exp2 = BufferUtils.getIncreasingByteArray(10); // testDir/foo/foobar1
    byte[] exp3 = BufferUtils.getIncreasingByteArray(20); // testDir/foo/foobar2
    byte[] expect = new byte[exp1.length + exp2.length + exp3.length];
    System.arraycopy(exp1, 0, expect, 0, exp1.length);
    System.arraycopy(exp2, 0, expect, exp1.length, exp2.length);
    System.arraycopy(exp3, 0, expect, exp1.length + exp2.length, exp3.length);

    int ret = sFsShell.run("cat", testDir + "/*/foo*");
    Assert.assertEquals(0, ret);
    Assert.assertArrayEquals(mOutput.toByteArray(), expect);
  }
}
