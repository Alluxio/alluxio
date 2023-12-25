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

import alluxio.Constants;
import alluxio.client.cli.fs.AbstractDoraFileSystemShellTest;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;
import alluxio.util.io.BufferUtils;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class DoraLsCommandIntegrationTest extends AbstractDoraFileSystemShellTest {
  public DoraLsCommandIntegrationTest() throws IOException {
    super(3);
  }

  @Override
  public void before() throws Exception {
    mLocalAlluxioClusterResource.setProperty(PropertyKey.MASTER_SCHEDULER_INITIAL_DELAY, "1s")
        .setProperty(PropertyKey.UNDERFS_XATTR_CHANGE_ENABLED, false);
    super.before();
  }

  private void createFiles() throws Exception {
    createByteFileInAlluxio("/testRoot/testFileA", BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    createByteFileInAlluxio("/testRoot/testFileB", BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    createByteFileInAlluxio("/testRoot/testDirectory/testFileC",
        BufferUtils.getIncreasingByteArray(Constants.MB), WritePType.CACHE_THROUGH);
    createByteFileInAlluxio("/testRoot/testDirectory/testFileD",
        BufferUtils.getIncreasingByteArray(Constants.MB), WritePType.THROUGH);
  }

  @Test
  public void testLsPath() throws Exception {
    createFiles();
    assertEquals(0, mFsShell.run("ls", "--sort", "path", "/testRoot"));
    checkOutput(
        ".*DIR /testRoot/testDirectory",
        ".*FILE /testRoot/testFileA",
        ".*FILE /testRoot/testFileB"
    );

    assertEquals(0, mFsShell.run("ls", "--sort", "path", "/testRoot/testDirectory"));
    checkOutput(
        ".*FILE /testRoot/testDirectory/testFileC",
        ".*FILE /testRoot/testDirectory/testFileD"
    );

    assertEquals(0, mFsShell.run("ls", "--sort", "path", "/testRoot/testDirectory/testFileC"));
    checkOutput(
        ".*FILE /testRoot/testDirectory/testFileC"
    );

    assertEquals(0, mFsShell.run("ls", "--sort", "path", "/testRoot/testDirectory/testFileD"));
    checkOutput(
        ".*FILE /testRoot/testDirectory/testFileD"
    );
  }

  @Test
  public void testLsMultipleDirs() throws Exception {
    createFiles();
    assertEquals(0, mFsShell.run("ls", "--sort", "path", "/testRoot/testDirectory", "/testRoot"));
    checkOutput(
        ".*FILE /testRoot/testDirectory/testFileC",
        ".*FILE /testRoot/testDirectory/testFileD",
        ".*DIR /testRoot/testDirectory",
        ".*FILE /testRoot/testFileA",
        ".*FILE /testRoot/testFileB"
    );
  }

  @Test
  public void testLsDirectory() throws Exception {
    createFiles();
    assertEquals(0, mFsShell.run("ls", "-d", "/testRoot"));
    checkOutput(
        ".*DIR /testRoot"
    );

    assertEquals(0, mFsShell.run("ls", "-d", "/testRoot/testDirectory"));
    checkOutput(
        ".*DIR /testRoot/testDirectory"
    );
  }

  @Test
  public void testLsRoot() throws Exception {
    createFiles();
    assertEquals(0, mFsShell.run("ls", "/"));
    checkOutput(
        ".*DIR /testRoot"
    );
  }

  @Test
  public void testLsWildcard() throws Exception {
    createFiles();
    assertEquals(0, mFsShell.run("ls", "/testRoot/testFile*"));
    checkOutput(
        ".*FILE /testRoot/testFileA",
        ".*FILE /testRoot/testFileB"
    );
  }

  @Test
  public void testLsR() throws Exception {
    createFiles();
    assertEquals(0, mFsShell.run("ls", "-R", "--sort", "path", "/testRoot"));
    checkOutput(
        ".*DIR /testRoot/testDirectory",
        ".*FILE /testRoot/testDirectory/testFileC",
        ".*FILE /testRoot/testDirectory/testFileD",
        ".*FILE /testRoot/testFileA",
        ".*FILE /testRoot/testFileB"
    );
  }

  @Ignore("Ignore broken test for now")
  @Test
  public void testLsWithSortByAccessTime() throws IOException, AlluxioException {
    String oldFile = "/testRoot/oldFile";
    String newFile = "/testRoot/newFile";
    createByteFileInAlluxio(oldFile, BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    createByteFileInAlluxio(newFile, BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    assertEquals(0, mFsShell.run("ls", "--sort", "lastAccessTime", "/testRoot"));
    checkOutput(
        ".*FILE " + oldFile,
        ".*FILE " + newFile
    );
  }

  @Test
  public void testLsWithSortBySize() throws IOException, AlluxioException {
    String smallFile = "/testRoot/smallFile";
    String largeFile = "/testRoot/largeFile";
    createByteFileInAlluxio(smallFile, BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    createByteFileInAlluxio(largeFile, BufferUtils.getIncreasingByteArray(2 * Constants.MB),
        WritePType.CACHE_THROUGH);
    assertEquals(0, mFsShell.run("ls", "--sort", "size", "/testRoot"));
    checkOutput(
        ".*FILE " + smallFile,
        ".*FILE " + largeFile
    );
  }

  @Test
  public void testLsWithSortBySizeAndReverse() throws IOException, AlluxioException {
    String smallFile = "/testRoot/smallFile";
    String largeFile = "/testRoot/largeFile";
    createByteFileInAlluxio(smallFile, BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    createByteFileInAlluxio(largeFile, BufferUtils.getIncreasingByteArray(2 * Constants.MB),
        WritePType.CACHE_THROUGH);
    assertEquals(0, mFsShell.run("ls", "--sort", "size", "-r", "/testRoot"));
    checkOutput(
        ".*FILE " + largeFile,
        ".*FILE " + smallFile
    );
  }

  @Ignore("Ignore broken test for now")
  @Test
  public void testLsWithSortByCreationTime() throws IOException, AlluxioException {
    String oldFile = "/testRoot/oldFile";
    String newFile = "/testRoot/newFile";
    createByteFileInAlluxio(oldFile, BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    createByteFileInAlluxio(newFile, BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    assertEquals(0, mFsShell.run("ls", "--sort", "creationTime", "/testRoot"));
    checkOutput(
        ".*FILE " + oldFile,
        ".*FILE " + newFile
    );
  }

  @Test
  public void testLsWithInvalidSortOption() throws IOException, AlluxioException {
    String oldFile = "/testRoot/oldFile";
    String newFile = "/testRoot/newFile";
    createByteFileInAlluxio(oldFile, BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    createByteFileInAlluxio(newFile, BufferUtils.getIncreasingByteArray(Constants.MB),
        WritePType.CACHE_THROUGH);
    assertEquals(-1, mFsShell.run("ls", "--sort", "invalidOption", "/testRoot"));
    String expected = "Invalid sort option `invalidOption` for --sort\n";
    assertEquals(expected, mOutput.toString());
  }

  private void checkOutput(String... linePatterns) {
    String[] actualLines = mOutput.toString().split("\n");
    assertEquals("Output: ", linePatterns.length, actualLines.length);

    for (int i = 0; i < linePatterns.length; i++) {
      assertTrue("mOutput: " + mOutput.toString(), actualLines[i].matches(linePatterns[i]));
    }
    mOutput.reset();
  }
}
