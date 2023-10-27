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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.cli.fs.AbstractDoraFileSystemShellTest;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemUtils;
import alluxio.conf.PropertyKey;
import alluxio.util.io.BufferUtils;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class DoraLoadCommandWithVirtualBlockIntegrationTest
    extends AbstractDoraFileSystemShellTest {

  public DoraLoadCommandWithVirtualBlockIntegrationTest() throws IOException {
    super(1);
  }

  @Override
  public void before() throws Exception {
    mLocalAlluxioClusterResource.setProperty(PropertyKey.MASTER_SCHEDULER_INITIAL_DELAY, "1s")
                                .setProperty(PropertyKey.DORA_READ_VIRTUAL_BLOCK_SIZE, "2MB")
                                .setProperty(PropertyKey.UNDERFS_XATTR_CHANGE_ENABLED, false);
    super.before();
  }

  @Test
  public void testCommand() throws Exception {
    File testRoot = mTestFolder.newFolder("testRoot");
    String path = testRoot.getAbsolutePath();
    mTestFolder.newFolder("testRoot/testDirectory");

    int lengthA = 1 * Constants.MB;
    createByteFileInUfs("/testRoot/testFileA", lengthA);
    int lengthB = 3 * Constants.MB;
    createByteFileInUfs("/testRoot/testFileB", lengthB);
    int lengthC = 5 * Constants.MB;
    createByteFileInUfs("/testRoot/testDirectory/testFileC", lengthC);

    AlluxioURI uriA = new AlluxioURI("/testRoot/testFileA");
    AlluxioURI uriB = new AlluxioURI("/testRoot/testFileB");
    AlluxioURI uriC = new AlluxioURI("/testRoot/testDirectory/testFileC");

    assertEquals(0, mFileSystem.getStatus(uriA).getInAlluxioPercentage());
    assertEquals(0, mFileSystem.getStatus(uriB).getInAlluxioPercentage());
    assertEquals(0, mFileSystem.getStatus(uriC).getInAlluxioPercentage());

    // Testing loading of a directory
    assertEquals(0, mFsShell.run("load", path, "--submit", "--verify"));
    assertEquals(0, mFsShell.run("load", path, "--progress"));

    FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uriA, 100);
    FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uriB, 100);
    FileSystemUtils.waitForAlluxioPercentage(mFileSystem, uriC, 100);
    FileInStream fileInStream = mFileSystem.openFile(uriA);
    byte[] buffer = new byte[lengthA];
    fileInStream.positionedRead(0, buffer, 0, lengthA);
    assertTrue(BufferUtils.equalIncreasingByteArray(lengthA, buffer));
    fileInStream = mFileSystem.openFile(uriB);
    buffer = new byte[lengthB];
    fileInStream.positionedRead(0, buffer, 0, lengthB);
    assertTrue(BufferUtils.equalIncreasingByteArray(lengthB, buffer));
    fileInStream = mFileSystem.openFile(uriC);
    buffer = new byte[lengthC];
    fileInStream.positionedRead(0, buffer, 0, lengthC);
    assertTrue(BufferUtils.equalIncreasingByteArray(lengthC, buffer));
    while (!mOutput.toString().contains("SUCCEEDED")) {
      assertEquals(0, mFsShell.run("load", path, "--progress"));
      Thread.sleep(1000);
    }
    assertTrue(mOutput.toString().contains("Inodes Processed: 4"));
    int bytes = (lengthA + lengthB + lengthC) / Constants.MB;
    assertTrue(mOutput.toString().contains(
        String.format("Bytes Loaded: %s.00MB out of %s.00MB", bytes, bytes)));
    assertTrue(mOutput.toString().contains("Files Failed: 0"));
    assertEquals(0, mFsShell.run("load", path, "--stop"));
    assertEquals(-2, mFsShell.run("load", "/testRootNotExists", "--progress"));
    assertTrue(mOutput.toString().contains("cannot be found."));
    mFsShell.run("load", path, "--progress", "--format", "JSON");
    assertTrue(mOutput.toString().contains("\"mJobState\":\"SUCCEEDED\""));
    mFsShell.run("load", path, "--progress", "--format", "JSON", "--verbose");
    assertTrue(mOutput.toString().contains("\"mVerbose\":true"));
  }
}
