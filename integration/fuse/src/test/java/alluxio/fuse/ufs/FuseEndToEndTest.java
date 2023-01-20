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

package alluxio.fuse.ufs;

import static alluxio.AlluxioTestDirectory.ALLUXIO_TEST_DIRECTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.util.CommonUtils;
import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;
import alluxio.util.WaitForOptions;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * Isolation tests for {@link alluxio.fuse.AlluxioJniFuseFileSystem} with local UFS.
 * This test covers the basic file system metadata operations.
 */
@Ignore("Under development")
public class FuseEndToEndTest extends AbstractFuseFileSystemTest {
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;

  /**
   * Runs FUSE with UFS related tests with different configuration combinations.
   *
   * @param localDataCacheEnabled     whether local data cache is enabled
   * @param localMetadataCacheEnabled whether local metadata cache is enabled
   */
  public FuseEndToEndTest(boolean localDataCacheEnabled, boolean localMetadataCacheEnabled) {
    super(localDataCacheEnabled, localMetadataCacheEnabled);
  }

  @Override
  public void fuseFileSystemBeforeActions() throws IOException {
    mFuseFs.mount(false, false, new HashSet<>());
    if (!waitForFuseMounted()) {
      umountFromShellIfMounted();
      fail("Could not setup FUSE mount point");
    }
  }

  @Override
  public void fuseFileSystemAfterActions() throws IOException {
    try {
      mFuseFs.umount(true);
    } catch (Exception e) {
      // will try umounting from shell
    }
    umountFromShellIfMounted();
  }

  private void umountFromShellIfMounted() throws IOException {
    if (fuseMounted()) {
      ShellUtils.execCommand("umount", mMountPoint);
    }
  }

  private boolean fuseMounted() throws IOException {
    String result = ShellUtils.execCommand("mount");
    return result.contains(mMountPoint);
  }

  /**
   * Waits for the Alluxio-Fuse to be mounted.
   *
   * @return true if Alluxio-Fuse mounted successfully in the given timeout, false otherwise
   */
  private boolean waitForFuseMounted() {
    if (OSUtils.isLinux() || OSUtils.isMacOS()) {
      try {
        CommonUtils.waitFor("Alluxio-Fuse mounted on local filesystem", () -> {
          try {
            return fuseMounted();
          } catch (IOException e) {
            return false;
          }
        }, WaitForOptions.defaults().setTimeoutMs(WAIT_TIMEOUT_MS));
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      } catch (TimeoutException te) {
        return false;
      }
    }
    return false;
  }

  @Test
  public void cat() throws Exception {
    String testFile = "/catTestFile";
    String content = "Alluxio Cat Test File Content";

    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }

    String result = ShellUtils.execCommand("cat", mMountPoint + testFile);
    assertEquals(content + "\n", result);
  }

  @Test
  public void chgrp() throws Exception {
    String testFile = "/chgrpTestFile";
    String userName = System.getProperty("user.name");
    Optional<String> groupName = AlluxioFuseUtils.getGroupName(userName);
    Assert.assertTrue(groupName.isPresent());
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);
    ShellUtils.execCommand("chgrp", groupName.get(), mMountPoint + testFile);
    assertEquals(groupName.get(), mFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
  }

  @Test
  public void chmod() throws Exception {
    String testFile = "/chmodTestFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);
    ShellUtils.execCommand("chmod", "777", mMountPoint + testFile);
    assertEquals((short) 0777, mFileSystem.getStatus(new AlluxioURI(testFile)).getMode());
  }

  @Test
  public void chown() throws Exception {
    String testFile = "/chownTestFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);

    String userName = System.getProperty("user.name");
    Optional<String> groupName = AlluxioFuseUtils.getGroupName(userName);
    Assert.assertTrue(groupName.isPresent());
    ShellUtils.execCommand("chown", userName + ":" + groupName.get(), mMountPoint + testFile);
    assertEquals(userName, mFileSystem.getStatus(new AlluxioURI(testFile)).getOwner());
    assertEquals(groupName.get(), mFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
  }

  @Test
  public void cp() throws Exception {
    String testFile = "/cpTestFile";
    String content = "Alluxio Cp Test File Content";
    File localFile = generateFileContent("/TestFileOnLocalPath", content.getBytes());

    ShellUtils.execCommand("cp", localFile.getPath(), mMountPoint + testFile);
    assertTrue(mFileSystem.exists(new AlluxioURI(testFile)));

    // Fuse release() is async
    // Cp again to make sure the first cp is completed
    String testFolder = "/cpTestFolder";
    ShellUtils.execCommand("mkdir", mMountPoint + testFolder);
    ShellUtils.execCommand("cp", mMountPoint + testFile, mMountPoint + testFolder + testFile);
    assertTrue(mFileSystem.exists(new AlluxioURI(testFolder + testFile)));

    byte[] read = new byte[content.length()];
    try (FileInStream is = mFileSystem.openFile(new AlluxioURI(testFile),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build())) {
      is.read(read);
    }
    assertEquals(content, new String(read, StandardCharsets.UTF_8));
  }

  @Test
  public void ddDuAndRm() throws Exception {
    String testFile = "/ddTestFile";
    createFileInFuse(testFile);

    // Fuse release() is async
    // Open the file to make sure dd is completed
    ShellUtils.execCommand("head", "-c", "10", mMountPoint + testFile);

    assertTrue(mFileSystem.exists(new AlluxioURI(testFile)));
    assertEquals(40 * Constants.KB, mFileSystem.getStatus(new AlluxioURI(testFile)).getLength());

    String output = ShellUtils.execCommand("du", "-k", mMountPoint + testFile);
    assertEquals("40", output.split("\\s+")[0]);

    ShellUtils.execCommand("rm", mMountPoint + testFile);
    assertFalse(mFileSystem.exists(new AlluxioURI(testFile)));
  }

  @Test
  public void head() throws Exception {
    String testFile = "/headTestFile";
    String content = "Alluxio Head Test File Content";
    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("head", "-c", "17", mMountPoint + testFile);
    assertEquals("Alluxio Head Test\n", result);
  }

  @Test
  public void ls() throws Exception {
    // ls -sh has different results in osx
    assumeTrue(OSUtils.isLinux());
    String testFile = "/lsTestFile";
    createFileInFuse(testFile);

    // Fuse getattr() will wait for file to be completed
    // when fuse release returns but does not finish
    String out = ShellUtils.execCommand("ls", "-sh", mMountPoint + testFile);
    assertFalse(out.isEmpty());
    assertEquals("40K", out.split("\\s+")[0]);

    assertTrue(mFileSystem.exists(new AlluxioURI(testFile)));
    assertEquals(40 * Constants.KB, mFileSystem.getStatus(new AlluxioURI(testFile)).getLength());
  }

  @Test
  public void mkdirAndMv() throws Exception {
    String testFile = "/mvTestFile";
    String testFolder = "/mkdirTestFolder";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);
    ShellUtils.execCommand("mkdir", mMountPoint + testFolder);
    ShellUtils.execCommand("mv", mMountPoint + testFile, mMountPoint + testFolder + testFile);
    assertFalse(mFileSystem.exists(new AlluxioURI(testFile)));
    assertTrue(mFileSystem.exists(new AlluxioURI(testFolder + testFile)));
  }

  @Test
  public void tail() throws Exception {
    String testFile = "/tailTestFile";
    String content = "Alluxio Tail Test File Content";
    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("tail", "-c", "17", mMountPoint + testFile);
    assertEquals("Test File Content\n", result);
  }

  @Test
  public void touchAndLs() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/lsTestFile", WritePType.MUST_CACHE, 10);
    String touchTestFile = "/touchTestFile";
    ShellUtils.execCommand("touch", mMountPoint + touchTestFile);

    String lsResult = ShellUtils.execCommand("ls", mMountPoint);
    assertTrue(lsResult.contains("lsTestFile"));
    assertTrue(lsResult.contains("touchTestFile"));
    assertTrue(mFileSystem.exists(new AlluxioURI(touchTestFile)));
  }

  /**
   * Create a file in Alluxio fuse mount point.
   *
   * @param filename the target file name
   */
  private void createFileInFuse(String filename) {
    try {
      ShellUtils.execCommand("dd", "if=/dev/zero", "of=" + mMountPoint + filename, "count=10",
          "bs=" + 4 * Constants.KB);
    } catch (IOException e) {
      fail();
    }
  }

  /**
   * Creates file by given path and writes content to file.
   *
   * @param path the file path
   * @param toWrite the file content
   * @return the created file instance
   * @throws FileNotFoundException if file not found
   */
  private File generateFileContent(String path, byte[] toWrite) throws IOException {
    File testFile = new File(ALLUXIO_TEST_DIRECTORY, path);
    Assert.assertTrue(testFile.createNewFile());
    try (FileOutputStream fos = new FileOutputStream(testFile)) {
      fos.write(toWrite);
    }
    return testFile;
  }
}
