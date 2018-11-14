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
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
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
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource
      = new LocalAlluxioClusterResource.Builder().build();

  private final boolean mFuseInstalled = AlluxioFuseUtils.isFuseInstalled();

  private AlluxioFuseFileSystem mFuseFileSystem;
  private String mMountPoint;
  private String mAlluxioRoot;
  private Thread mFuseThread;
  private FileSystem mFileSystem;

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

    waitForFuseMounted();
    if (!waitForFuseMounted()) {
      // Fuse may take longer to mount and still need to umount
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
    FileOutStream os = mFileSystem.createFile(new AlluxioURI("/CatTestFile"));
    os.write("Alluxio Cat Test File Content".getBytes());
    os.close();
    String result = ShellUtils.execCommand("cat", mMountPoint + "/CatTestFile");
    Assert.assertEquals("Alluxio Cat Test File Content\n", result);
  }

  @Test
  public void chmod() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/ChmodTestFile", WriteType.MUST_CACHE, 10);
    ShellUtils.execCommand("chmod", "700", mMountPoint + "/ChmodTestFile");
    Assert.assertEquals(448, mFileSystem.getStatus(new AlluxioURI("/ChmodTestFile")).getMode());
  }

  @Test
  public void cp() throws Exception {
    String dataString = "copyFromLocalFileToDstPathTest";
    byte[] data = dataString.getBytes();
    File localFile = generateFileContent("/CpTestFile", data);
    ShellUtils.execCommand("cp", localFile.getPath(), mMountPoint + "/CpTestFile");
    Assert.assertEquals("CpTestFile\n", ShellUtils.execCommand("ls", mMountPoint));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/CpTestFile")));
  }

  @Test
  public void head() throws Exception {
    FileOutStream os = mFileSystem.createFile(new AlluxioURI("/HeadTestFile"));
    os.write("Alluxio Head Test File Content".getBytes());
    os.close();
    String result = ShellUtils.execCommand("head", "-c", "17", mMountPoint + "/HeadTestFile");
    Assert.assertEquals("Alluxio Head Test\n", result);
  }

  @Test
  public void ls() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/LsTestFile", WriteType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(mFileSystem, "/LsTestFile2", WriteType.CACHE_THROUGH, 10);
    Assert.assertEquals("LsTestFile\nLsTestFile2\n", ShellUtils.execCommand("ls", mMountPoint));
  }

  @Test
  public void mv() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/MkdirTestFile", WriteType.MUST_CACHE, 10);
    ShellUtils.execCommand("mkdir", mMountPoint + "/testFolder");
    ShellUtils.execCommand("mv", mMountPoint + "/MkdirTestFile",
        mMountPoint + "/testFolder/MkdirTestFile");
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI("/MkdirTestFile")));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI("/testFolder/MkdirTestFile")));
  }

  @Test
  public void tail() throws Exception {
    FileOutStream os = mFileSystem.createFile(new AlluxioURI("/TailTestFile"));
    os.write("Alluxio Tail Test File Content".getBytes());
    os.close();
    String result = ShellUtils.execCommand("tail", "-c", "17", mMountPoint + "/TailTestFile");
    Assert.assertEquals("Test File Content\n", result);
  }

  @Test
  public void touch() throws Exception {
    ShellUtils.execCommand("touch", mMountPoint + "/CpTestFile.txt");
    Assert.assertEquals("CpTestFile.txt\n", ShellUtils.execCommand("ls", mMountPoint));
    Assert.assertNotNull(mFileSystem.getStatus(new AlluxioURI("/CpTestFile.txt")));
  }

  /**
   * Umounts the Alluxio-Fuse.
   */
  private void umountFuse() throws InterruptedException {
    mFuseFileSystem.umount();
    mFuseThread.interrupt();
    mFuseThread.join(5000);
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
            result = ShellUtils.execCommand("mount", "|", "grep", mMountPoint);
          } catch (IOException e) {
            return false;
          }
          return !result.isEmpty();
        }, WaitForOptions.defaults().setTimeoutMs(5000));
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
