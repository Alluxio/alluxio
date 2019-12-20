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
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.WritePType;
import alluxio.testutils.underfs.ConfExpectingUnderFileSystemFactory;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Tests for mount command.
 */
public final class MountCommandTest extends AbstractFileSystemShellTest {
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
    sFileSystem.createDirectory(testDir,
        CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build());
    Assert.assertTrue(fileExists(testDir));
    Assert.assertTrue(Files.exists(Paths.get(ufsPath, "testDir")));
  }

  @Test
  public void mount() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, sFsShell.run("mount", mountPoint.toString(), ufsPath));
    checkMountPoint(mountPoint, ufsPath);
  }

  @Test
  public void mountWithWrongArgs() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    // ufs missing
    Assert.assertEquals(-1, sFsShell.run("mount", mountPoint.toString()));
    // extra arg
    Assert.assertEquals(-1, sFsShell.run("mount", mountPoint.toString(), ufsPath, "extraArg"));
    // --option with wrong argument format
    Assert.assertEquals(-1,
        sFsShell.run("mount", "--option", "wrongArgFormat", mountPoint.toString(), ufsPath));
  }

  @Test
  public void mountWithMultipleOptions() throws Exception {
    ConfExpectingUnderFileSystemFactory factory =
        new ConfExpectingUnderFileSystemFactory("ufs", ImmutableMap.of("k1", "v1", "k2", "v2"));
    try (Closeable closeable =
        new UnderFileSystemFactoryRegistryRule(factory).toResource()) {
      AlluxioURI mountPoint = new AlluxioURI("/mnt");
      String ufsPath = "ufs://" + mFolder.getRoot().getAbsolutePath();
      Assert.assertEquals(0, sFsShell
          .run("mount", "--option", "k1=v1", "--option", "k2=v2", mountPoint.toString(), ufsPath));
      FileSystemTestUtils.createByteFile(sFileSystem,
          "/mnt/testFile1", WritePType.CACHE_THROUGH, 20);
      Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/mnt/testFile1")));
      URIStatus status = sFileSystem.getStatus(new AlluxioURI("/mnt/testFile1"));
      Assert.assertTrue(status.isPersisted());
    }
  }

  @Test
  public void mountWithWrongOptions() throws Exception {
    ConfExpectingUnderFileSystemFactory factory =
        new ConfExpectingUnderFileSystemFactory("ufs", ImmutableMap.of("k1", "v1", "k2", "v2"));
    try (Closeable closeable =
        new UnderFileSystemFactoryRegistryRule(factory).toResource()) {
      AlluxioURI mountPoint = new AlluxioURI("/mnt");
      String ufsPath = "ufs://" + mFolder.getRoot().getAbsolutePath();
      // one property is wrong
      Assert.assertEquals(-1, sFsShell
          .run("mount", "--option", "k1=not_v1", "--option", "k2=v2", mountPoint.toString(),
              ufsPath));
      // one property is missing
      Assert.assertEquals(-1,
          sFsShell.run("mount", "--option", "k1=v1", mountPoint.toString(), ufsPath));
      // one property is unnecessary
      Assert.assertEquals(-1, sFsShell
          .run("mount", "--option", "k1=v1", "--option", "k2=v2", "--option", "k3=v3",
              mountPoint.toString(), ufsPath));
    }
  }

  @Test
  public void mountWithSpaceInOptionValues() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, sFsShell
        .run("mount", "--option", "key=\" value with spaces\"", mountPoint.toString(), ufsPath));
  }

  @Test
  public void mountWithQuotesInOptionValues() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, sFsShell
        .run("mount", "--option", "key=valueWith\"Quotes\"", mountPoint.toString(), ufsPath));
  }

  @Test
  public void mountWithEqualsInOptionValues() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0,
        sFsShell.run("mount", "--option", "key=k=v", mountPoint.toString(), ufsPath));
  }

  @Test
  public void mountToRootFailed() throws Exception {
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(-1, sFsShell.run("mount", "/", ufsPath));
  }

  @Test
  public void mountToExistingMountPointFailed() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath1 = mFolder.newFolder().getAbsolutePath();
    String ufsPath2 = mFolder.newFolder().getAbsolutePath();
    Assert.assertEquals(0, sFsShell.run("mount", mountPoint.toString(), ufsPath1));
    Assert.assertEquals(-1, sFsShell.run("mount", mountPoint.toString(), ufsPath2));
  }

  @Test
  public void mountTwiceFailed() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, sFsShell.run("mount", mountPoint.toString(), ufsPath));
    Assert.assertEquals(-1, sFsShell.run("mount", mountPoint.toString(), ufsPath));
  }
}
