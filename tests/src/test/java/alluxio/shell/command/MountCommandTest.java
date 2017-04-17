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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Tests for mount command.
 */
public final class MountCommandTest extends AbstractAlluxioShellTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private void checkMountPoint(AlluxioURI mountPoint, String ufsPath) throws Exception {
    // File in UFS can be read in Alluxio
    AlluxioURI testFile = mountPoint.join("testFile");
    generateRelativeFileContent(PathUtils.concatPath(ufsPath, "testFile"),
        BufferUtils.getIncreasingByteArray(10));
    Assert.assertTrue(fileExists(testFile));
    // Dir in Alluxio can be persisted to UFS
    AlluxioURI testDir = mountPoint.join("testDir");
    mFileSystem.createDirectory(testDir,
        CreateDirectoryOptions.defaults().setWriteType(WriteType.CACHE_THROUGH));
    Assert.assertTrue(fileExists(testDir));
    Assert.assertTrue(Files.exists(Paths.get(ufsPath, "testDir")));
  }

  @Test
  public void mount() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, mFsShell.run("mount", mountPoint.toString(), ufsPath));
    checkMountPoint(mountPoint, ufsPath);
  }

  @Test
  public void mountWithWrongArgs() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    // alluxio, ufs path missing
    Assert.assertEquals(-1, mFsShell.run("mount"));
    // ufs missing
    Assert.assertEquals(-1, mFsShell.run("mount", mountPoint.toString()));
    // extra arg
    Assert.assertEquals(-1, mFsShell.run("mount", mountPoint.toString(), ufsPath, "extraArg"));
    // --option with wrong argument format
    Assert.assertEquals(-1,
        mFsShell.run("mount", "--option", "wrongArgFormat", mountPoint.toString(), ufsPath));
  }

  @Test
  public void mountWithMultipleOptions() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, mFsShell
        .run("mount", "--option", "k1=v1", "--option", "k2=v2", mountPoint.toString(), ufsPath));
  }

  @Test
  public void mountWithSpaceQuotedOption() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, mFsShell
        .run("mount", "--option", "key=\" value with spaces\"", mountPoint.toString(), ufsPath));
  }

  @Test
  public void mountToRootFailed() throws Exception {
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(-1, mFsShell.run("mount", "/", ufsPath));
  }

  @Test
  public void mountToExistingMountPointFailed() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath1 = mFolder.newFolder().getAbsolutePath();
    String ufsPath2 = mFolder.newFolder().getAbsolutePath();
    Assert.assertEquals(0, mFsShell.run("mount", mountPoint.toString(), ufsPath1));
    Assert.assertEquals(-1, mFsShell.run("mount", mountPoint.toString(), ufsPath2));
  }

  @Test
  public void mountTwiceFailed() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, mFsShell.run("mount", mountPoint.toString(), ufsPath));
    Assert.assertEquals(-1, mFsShell.run("mount", mountPoint.toString(), ufsPath));
  }
}
