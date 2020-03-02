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
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
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
 * Tests for updateMount command.
 */
public final class UpdateMountCommandTest extends AbstractFileSystemShellTest {
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
  public void updateMount() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, sFsShell.run("mount", mountPoint.toString(), ufsPath));
    Assert.assertEquals(0, sFsShell.run("updateMount", "--option", "k1=v1",
        mountPoint.toString()));
    checkMountPoint(mountPoint, ufsPath);
  }

  @Test
  public void updateMountWithWrongArgs() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, sFsShell.run("mount", mountPoint.toString(), ufsPath));
    // extra arg
    Assert.assertEquals(-1, sFsShell.run("updateMount", mountPoint.toString(), "extraArg"));
    // --option with wrong argument format
    Assert.assertEquals(-1,
        sFsShell.run("updateMount", "--option", "wrongArgFormat", mountPoint.toString()));
  }

  @Test
  public void updateMountWithMultipleOptions() throws Exception {
    ConfExpectingUnderFileSystemFactory factory =
        new ConfExpectingUnderFileSystemFactory("ufs", ImmutableMap.of("k1", "v1", "k2", "v2"));
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = "ufs://" + mFolder.getRoot().getAbsolutePath();
    try (Closeable closeable =
        new UnderFileSystemFactoryRegistryRule(factory).toResource()) {
      Assert.assertEquals(0, sFsShell
          .run("mount", "--option", "k1=v1", "--option", "k2=v2", mountPoint.toString(), ufsPath));
      FileSystemTestUtils.createByteFile(sFileSystem,
          "/mnt/testFile1", WritePType.CACHE_THROUGH, 20);
      Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/mnt/testFile1")));
      URIStatus status = sFileSystem.getStatus(new AlluxioURI("/mnt/testFile1"));
      Assert.assertTrue(status.isPersisted());
    }

    ConfExpectingUnderFileSystemFactory newFactory =
        new ConfExpectingUnderFileSystemFactory("ufs", ImmutableMap.of("k1", "v3", "k2", "v4"));
    try (Closeable closeable =
             new UnderFileSystemFactoryRegistryRule(newFactory).toResource()) {
      Assert.assertEquals(0, sFsShell
          .run("updateMount", "--option", "k1=v3", "--option", "k2=v4", mountPoint.toString()));
      FileSystemTestUtils.createByteFile(sFileSystem,
          "/mnt/testFile2", WritePType.CACHE_THROUGH, 20);
      Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/mnt/testFile2")));
      URIStatus status = sFileSystem.getStatus(new AlluxioURI("/mnt/testFile2"));
      Assert.assertTrue(status.isPersisted());
    }
  }

  @Test
  public void updateMountWithWrongOptionsAndThenCorrected() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = "ufs://" + mFolder.getRoot().getAbsolutePath();
    ImmutableMap<String, String> options = ImmutableMap.of("k1", "v1", "k2", "v2");
    ConfExpectingUnderFileSystemFactory factory =
        new ConfExpectingUnderFileSystemFactory("ufs", options);
    try (Closeable closeable =
             new UnderFileSystemFactoryRegistryRule(factory).toResource()) {
      Assert.assertEquals(0, sFsShell
          .run("mount", "--option", "k1=v1", "--option", "k2=v2", mountPoint.toString(), ufsPath));
      Assert.assertEquals(-1, sFsShell
          .run("updateMount", "--option", "k1=not_v1", "--option", "k2=v2", mountPoint.toString()));
      boolean gotException = false;
      // check mount point is still usable if updateMount fails
      FileSystemTestUtils.createByteFile(sFileSystem,
          "/mnt/testFile1", WritePType.CACHE_THROUGH, 20);
      // check mount point options are not changed
      Assert.assertEquals(options,
          sFileSystem.getMountTable().get(mountPoint.getPath()).getProperties());
    }
    ImmutableMap<String, String> options2 = ImmutableMap.of("k1", "v3", "k2", "v4");
    ConfExpectingUnderFileSystemFactory factory2 =
        new ConfExpectingUnderFileSystemFactory("ufs", options2);
    try (Closeable closeable =
             new UnderFileSystemFactoryRegistryRule(factory2).toResource()) {
      Assert.assertEquals(0, sFsShell
          .run("updateMount", "--option", "k1=v3", "--option", "k2=v4", mountPoint.toString()));
      // check mount point is in working state after updated with correct options
      FileSystemTestUtils.createByteFile(sFileSystem,
          "/mnt/testFile2", WritePType.CACHE_THROUGH, 20);
      Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/mnt/testFile2")));
      URIStatus status = sFileSystem.getStatus(new AlluxioURI("/mnt/testFile2"));
      Assert.assertTrue(status.isPersisted());
      // check mount point options are not changed
      Assert.assertEquals(options2,
          sFileSystem.getMountTable().get(mountPoint.getPath()).getProperties());
    }
  }

  @Test
  public void updateMountWithSpaceInOptionValues() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, sFsShell
        .run("mount", "--option", "key=\" value with spaces\"", mountPoint.toString(), ufsPath));
    Assert.assertEquals(0, sFsShell
        .run("updateMount", "--option", "key=\" value with spaces 2\"", mountPoint.toString()));
  }

  @Test
  public void updateMountWithQuotesInOptionValues() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0, sFsShell
        .run("mount", "--option", "key=valueWith\"Quotes\"", mountPoint.toString(), ufsPath));
    Assert.assertEquals(0, sFsShell
        .run("updateMount", "--option", "key=valueWith\"Quotes2\"", mountPoint.toString()));
  }

  @Test
  public void updateMountWithEqualsInOptionValues() throws Exception {
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(0,
        sFsShell.run("mount", "--option", "key=k=v", mountPoint.toString(), ufsPath));
    Assert.assertEquals(0,
        sFsShell.run("updateMount", "--option", "key=k=v2", mountPoint.toString()));
  }

  @Test
  public void updateRootMountFailed() throws Exception {
    String ufsPath = mFolder.getRoot().getAbsolutePath();
    Assert.assertEquals(-1, sFsShell.run("updateMount", "/", ufsPath));
  }
}
