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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.WritePType;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;

import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for checkConsistency command.
 */
public class CheckConsistencyCommandIntegrationTest extends AbstractFileSystemShellTest {
  /**
   * Tests the check consistency shell command correctly identifies a consistent subtree.
   */
  @Test
  public void consistent() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testFileA",
        WritePType.CACHE_THROUGH, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir/testFileB",
        WritePType.CACHE_THROUGH, 20);
    sFsShell.run("checkConsistency", "/testRoot");
    String expected = "/testRoot is consistent with the under storage system.\n";
    assertEquals(expected, mOutput.toString());
    mOutput.reset();
    sFsShell.run("checkConsistency", "-r", "/testRoot");
    assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests the check consistency shell command correctly identifies inconsistent files in a subtree.
   */
  @Test
  public void inconsistent() throws Exception {
    FileSystemTestUtils
        .createByteFile(sFileSystem, "/testRoot/testFileA", WritePType.CACHE_THROUGH, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir/testFileB",
            WritePType.CACHE_THROUGH, 20);
    String ufsPath = sFileSystem.getStatus(new AlluxioURI("/testRoot/testDir")).getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    ufs.deleteDirectory(ufsPath, DeleteOptions.defaults().setRecursive(true));
    sFsShell.run("checkConsistency", "/testRoot");
    StringBuilder expected = new StringBuilder();
    expected.append("The following files are inconsistent:\n");
    expected.append("/testRoot/testDir\n");
    expected.append("/testRoot/testDir/testFileB\n");
    assertEquals(expected.toString(), mOutput.toString());

    mOutput.reset();
    sFsShell.run("checkConsistency", "-r", "/testRoot");
    String res = mOutput.toString();
    assertTrue(res.contains("/testRoot" + " has: " + "2 inconsistent files.")
        && res.contains("repairing path: " + "/testRoot/testDir\n")
        && res.contains("repairing path: " + "/testRoot/testDir/testFileB\n"));
    assertFalse(sFileSystem.exists(new AlluxioURI("/testRoot/testDir")));
    assertFalse(sFileSystem.exists(new AlluxioURI("/testRoot/testDir/testFileB")));

    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir/testFileB",
            WritePType.CACHE_THROUGH, 20);
    ufsPath = sFileSystem.getStatus(new AlluxioURI("/testRoot/testDir/testFileB")).getUfsPath();
    ufs.deleteFile(ufsPath);
    OutputStream outputStream = ufs.create(ufsPath);
    byte[] bytes = {1, 2, 3};
    outputStream.write(bytes);
    outputStream.close();
    mOutput.reset();
    sFsShell.run("checkConsistency", "-r", "/testRoot");
    res = mOutput.toString();
    assertTrue(res.contains("/testRoot" + " has: " + "1 inconsistent files.")
        && res.contains("repairing path: " + "/testRoot/testDir/testFileB\n"));
    assertTrue(sFileSystem.exists(new AlluxioURI("/testRoot/testDir/testFileB"),
        ExistsPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).build()));
    assertEquals(3, sFileSystem.getStatus(new AlluxioURI("/testRoot/testDir/testFileB"))
        .getLength());
  }

  /**
   * Tests the check consistency shell command correctly identifies a consistent subtree.
   */
  @Test
  public void wildcard() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir2/testFileA",
        WritePType.CACHE_THROUGH, 10);

    FileSystemTestUtils.createByteFile(sFileSystem, "/testRoot/testDir/testFileB",
        WritePType.CACHE_THROUGH, 20);
    sFsShell.run("checkConsistency", "/testRoot/*/testFile*");
    String expected = "/testRoot/testDir/testFileB is consistent with the under storage system.\n"
            + "/testRoot/testDir2/testFileA is consistent "
            + "with the under storage system.\n";
    assertEquals(expected, mOutput.toString());
  }

  @Test
  public void multiThreaded() throws Exception {

    makeInconsistentFiles("/testRoot", 1);
    sFsShell.run("checkConsistency -r -t 1 /testRoot".split(" "));
    String res = mOutput.toString();
    assertTrue(res.contains("Repairing with 1 threads."));
    mOutput.reset();

    makeInconsistentFiles("/testRoot", 2);
    sFsShell.run("checkConsistency -r -t 2 /testRoot".split(" "));
    res = mOutput.toString();
    assertTrue(res.contains("Repairing with 2 threads."));
  }

  void makeInconsistentFiles(String rootDir, int nFiles) throws AlluxioException, IOException {
    List<String> filenames = new ArrayList<>();
    for (int i = 0; i < nFiles; i++) {
      filenames.add(String.format("%s/testDir/testFile%d", rootDir, i));
    }
    filenames.forEach((path) -> FileSystemTestUtils
        .createByteFile(sFileSystem, path, WritePType.CACHE_THROUGH, 10));

    String ufsPath = sFileSystem.getStatus(new AlluxioURI("/testRoot/testDir")).getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath, ServerConfiguration.global());
    ufs.deleteDirectory(ufsPath, DeleteOptions.defaults().setRecursive(true));
  }
}
