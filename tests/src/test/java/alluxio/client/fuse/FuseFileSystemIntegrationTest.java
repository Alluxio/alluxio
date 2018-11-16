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

package alluxio.client.fuse;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.fuse.AlluxioFuseFileSystem;
import alluxio.fuse.AlluxioFuseOptions;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;
import alluxio.util.WaitForOptions;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

/**
 * Integration tests for {@link AlluxioFuseFileSystem}.
 *
 * This test only runs when libfuse (linux) or osxfuse (osxfuse) is installed on local machine.
 */
public class FuseFileSystemIntegrationTest {
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource
      = new LocalAlluxioClusterResource.Builder().build();

  private final boolean mFuseInstalled = AlluxioFuseUtils.isFuseInstalled();

  private FileSystem mFileSystem;
  private AlluxioFuseFileSystem mFuseFileSystem;
  private Thread mFuseThread;
  private String mMountPoint;
  private String mAlluxioRoot;

  @Before
  public void before() throws Exception {
    Assume.assumeTrue(mFuseInstalled);

    // Mount Alluxio root to a temp directory
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mAlluxioRoot = "/";
    mMountPoint = AlluxioTestDirectory
        .createTemporaryDirectory("FuseMountPoint").getAbsolutePath();

    AlluxioFuseOptions options = new AlluxioFuseOptions(mMountPoint,
        mAlluxioRoot, false, new ArrayList<>());
    mFuseFileSystem = new AlluxioFuseFileSystem(mFileSystem, options);
    mFuseThread = new Thread(() -> mFuseFileSystem.mount(Paths.get(mMountPoint),
        true, false, new String[]{"-odirect_io"}));
    mFuseThread.start();

    if (!waitForFuseMounted()) {
      // Fuse may not be mounted within timeout and we need to umount it
      umountFuse();
      Assume.assumeTrue(false);
    }
  }

  @After
  public void after() throws Exception {
    if (mFuseInstalled) {
      umountFuse();
    }
  }

  @Test
  public void cat() throws Exception {
    String testFile = "/catTestFile";
    String content = "Alluxio Cat Test File Content";

    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }

    String result = ShellUtils.execCommand("cat", mMountPoint + testFile);
    Assert.assertEquals(content + "\n", result);
  }

  @Test
  public void chgrp() throws Exception {
    // Osxfuse does not support chgrp
    Assume.assumeTrue(OSUtils.isLinux());
    String testFile = "/chgrpTestFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WriteType.MUST_CACHE, 10);
    String userName = System.getProperty("user.name");
    String groupName = AlluxioFuseUtils.getGroupName(userName);
    ShellUtils.execCommand("chgrp", groupName, mMountPoint + testFile);
    Assert.assertNotEquals(groupName, mFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
  }

  @Test
  public void chmod() throws Exception {
    String testFile = "/chmodTestFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WriteType.MUST_CACHE, 10);
    ShellUtils.execCommand("chmod", "777", mMountPoint + testFile);
    Assert.assertEquals((short) 0777, mFileSystem.getStatus(new AlluxioURI(testFile)).getMode());
  }

  @Test
  public void chown() throws Exception {
    // Osxfuse does not support chown
    Assume.assumeTrue(OSUtils.isLinux());
    String testFile = "/chownTestFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WriteType.MUST_CACHE, 10);
    String userName = System.getProperty("user.name");
    String groupName = AlluxioFuseUtils.getGroupName(userName);
    ShellUtils.execCommand("chown", userName + ":" + groupName, mMountPoint + testFile);
    Assert.assertNotEquals(userName, mFileSystem.getStatus(new AlluxioURI(testFile)).getOwner());
    Assert.assertNotEquals(groupName, mFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
  }

  @Test
  public void cp() throws Exception {
    String testFile = "/cpTestFile";
    String content = "Alluxio Cp Test File Content";
    byte[] contentArray = content.getBytes();
    File localFile = generateFileContent("/TestFileOnLocalPath", contentArray);

    ShellUtils.execCommand("cp", localFile.getPath(), mMountPoint + testFile);
    Assert.assertEquals("cpTestFile\n", ShellUtils.execCommand("ls", mMountPoint));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testFile)));

    byte[] read = new byte[content.length()];
    try (FileInStream is = mFileSystem.openFile(new AlluxioURI(testFile),
        OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE))) {
      is.read(read);
    }
    Assert.assertArrayEquals(contentArray, read);
  }

  @Test
  public void dd() throws Exception {
    String testFile = "/ddTestFile";
    ShellUtils.execCommand("dd", "if=/dev/zero",
        "of=" + mMountPoint + testFile, "count=1024", "bs=" + Constants.MB);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testFile)));
    Assert.assertEquals(Constants.GB, mFileSystem.getStatus(new AlluxioURI(testFile)).getLength());
  }

  @Test
  public void head() throws Exception {
    String testFile = "/headTestFile";
    String content = "Alluxio Head Test File Content";
    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("head", "-c", "17", mMountPoint + "/HeadTestFile");
    Assert.assertEquals("Alluxio Head Test\n", result);
  }

  @Test
  public void ls() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/lsTestFile", WriteType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(mFileSystem, "/lsTestFile2",
        WriteType.CACHE_THROUGH, 10);
    ShellUtils.execCommand("touch", mMountPoint + "/lsTestFile3");
    Assert.assertEquals("lsTestFile\nlsTestFile2\nlsTestFile3\n",
        ShellUtils.execCommand("ls", mMountPoint));
  }

  @Test
  public void mv() throws Exception {
    String testFile = "/mvTestFile";
    String testFolder = "/testFolder";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WriteType.MUST_CACHE, 10);
    ShellUtils.execCommand("mkdir", mMountPoint + testFolder);
    ShellUtils.execCommand("mv", mMountPoint + testFile,
        mMountPoint + testFolder + testFile);
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI(testFile)));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testFolder + testFile)));
  }

  @Test
  public void tail() throws Exception {
    String testFile = "/tailTestFile";
    String content = "Alluxio Tail Test File Content";
    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("tail", "-c", "17", mMountPoint + testFile);
    Assert.assertEquals("Test File Content\n", result);
  }

  /**
   * Umounts the Alluxio-Fuse.
   */
  private void umountFuse() throws InterruptedException {
    mFuseFileSystem.umount();
    mFuseThread.interrupt();
    mFuseThread.join();
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
    File testFile = new File(mLocalAlluxioClusterResource.get().getAlluxioHome() + path);
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(toWrite);
    fos.close();
    return testFile;
  }

  /**
   * Waits for the Alluxio-Fuse to be mounted.
   *
   * @return true if Alluxio-Fuse mounted successfully in the given timeout, false otherwise
   */
  private boolean waitForFuseMounted() throws IOException {
    if (OSUtils.isLinux() || OSUtils.isMacOS()) {
      try {
        CommonUtils.waitFor("Alluxio-Fuse mounted on local filesystem", () -> {
          String result;
          try {
            result = ShellUtils.execCommand("bash", "-c", "mount | grep " + mMountPoint);
          } catch (IOException e) {
            return false;
          }
          return !result.isEmpty();
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
}
