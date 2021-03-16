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

import static org.junit.Assert.fail;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;
import alluxio.util.WaitForOptions;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * This is the base class of Fuse tests. It describes the POSIX API functionalities
 * that all Fuse implementations and launch ways should support. Each Fuse implementation
 * and launch ways is expected to create a test that extends this base class.
 */
public abstract class AbstractFuseIntegrationTest {
  private static final String ALLUXIO_ROOT = "/";
  private static final int BLOCK_SIZE = 4 * Constants.KB;
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;

  private LocalAlluxioCluster mAlluxioCluster;
  private FileSystem mFileSystem;
  private String mMountPoint;

  @Rule
  public TestName mTestName = new TestName();

  /**
   * Creates the local Alluxio cluster for testing.
   *
   * @param blockSize the block size
   * @param mountPath the Fuse mount path
   * @param alluxioRoot the Fuse mounted alluxio root
   * @return the created local Alluxio cluster
   * @throws Exception if the cluster cannot be started
   */
  public abstract LocalAlluxioCluster createLocalAlluxioCluster(String clusterName, int blockSize,
      String mountPath, String alluxioRoot) throws Exception;

  /**
   * Manually mounts the Fuse application if needed.
   */
  public abstract void mountFuse(FileSystem fileSystem, String mountPoint, String alluxioRoot);

  /**
   * Try to umount the given mount path.
   *
   * @throws Exception if the fuse is already unmounted or the mount path does not exist
   */
  public abstract void umountFuse(String mountPath) throws Exception;

  @BeforeClass
  public static void beforeClass() {
    // This test only runs when fuse is installed
    boolean fuseInstalled = AlluxioFuseUtils.isFuseInstalled();
    Assume.assumeTrue(fuseInstalled);
  }

  @Before
  public void before() throws Exception {
    String clusterName = IntegrationTestUtils
        .getTestName(getClass().getSimpleName(), mTestName.getMethodName());
    mMountPoint = AlluxioTestDirectory
        .createTemporaryDirectory(clusterName).getAbsolutePath();
    mAlluxioCluster = createLocalAlluxioCluster(clusterName, BLOCK_SIZE, mMountPoint, ALLUXIO_ROOT);
    mFileSystem = mAlluxioCluster.getClient();
    mountFuse(mFileSystem, mMountPoint, ALLUXIO_ROOT);
    if (!waitForFuseMounted()) {
      stopClusterAndUmountFuse();
      Assume.assumeTrue(false);
    }
  }

  @After
  public void after() throws Exception {
    stopClusterAndUmountFuse();
  }

  private void stopClusterAndUmountFuse() throws Exception {
    mAlluxioCluster.stop();
    if (fuseMounted()) {
      try {
        umountFuse(mMountPoint);
      } catch (Exception e) {
        // The Fuse application may be unmounted by the cluster stop
      }
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
    String testFile = "/chgrpTestFile";
    String userName = System.getProperty("user.name");
    String groupName = AlluxioFuseUtils.getGroupName(userName);
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);
    ShellUtils.execCommand("chgrp", groupName, mMountPoint + testFile);
    Assert.assertEquals(groupName, mFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
  }

  @Test
  public void chmod() throws Exception {
    String testFile = "/chmodTestFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);
    ShellUtils.execCommand("chmod", "777", mMountPoint + testFile);
    Assert.assertEquals((short) 0777, mFileSystem.getStatus(new AlluxioURI(testFile)).getMode());
  }

  @Test
  public void chown() throws Exception {
    String testFile = "/chownTestFile";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);

    String userName = System.getProperty("user.name");
    String groupName = AlluxioFuseUtils.getGroupName(userName);
    ShellUtils.execCommand("chown", userName + ":" + groupName, mMountPoint + testFile);
    Assert.assertEquals(userName, mFileSystem.getStatus(new AlluxioURI(testFile)).getOwner());
    Assert.assertEquals(groupName, mFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
  }

  @Test
  public void cp() throws Exception {
    String testFile = "/cpTestFile";
    String content = "Alluxio Cp Test File Content";
    File localFile = generateFileContent("/TestFileOnLocalPath", content.getBytes());

    ShellUtils.execCommand("cp", localFile.getPath(), mMountPoint + testFile);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testFile)));

    // Fuse release() is async
    // Cp again to make sure the first cp is completed
    String testFolder = "/cpTestFolder";
    ShellUtils.execCommand("mkdir", mMountPoint + testFolder);
    ShellUtils.execCommand("cp", mMountPoint + testFile, mMountPoint + testFolder + testFile);
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testFolder + testFile)));

    byte[] read = new byte[content.length()];
    try (FileInStream is = mFileSystem.openFile(new AlluxioURI(testFile),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build())) {
      is.read(read);
    }
    Assert.assertEquals(content, new String(read, "UTF8"));
  }

  @Test
  public void ddDuAndRm() throws Exception {
    String testFile = "/ddTestFile";
    createFileInFuse(testFile);

    // Fuse release() is async
    // Open the file to make sure dd is completed
    ShellUtils.execCommand("head", "-c", "10", mMountPoint + testFile);

    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testFile)));
    Assert.assertEquals(40 * Constants.KB,
        mFileSystem.getStatus(new AlluxioURI(testFile)).getLength());

    String output = ShellUtils.execCommand("du", "-k", mMountPoint + testFile);
    Assert.assertEquals("40", output.split("\\s+")[0]);

    ShellUtils.execCommand("rm", mMountPoint + testFile);
    Assert.assertFalse(mFileSystem.exists(new AlluxioURI(testFile)));
  }

  @Test
  public void head() throws Exception {
    String testFile = "/headTestFile";
    String content = "Alluxio Head Test File Content";
    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("head", "-c", "17", mMountPoint + testFile);
    Assert.assertEquals("Alluxio Head Test\n", result);
  }

  @Test
  public void ls() throws Exception {
    // ls -sh has different results in osx
    Assume.assumeTrue(OSUtils.isLinux());
    String testFile = "/lsTestFile";
    createFileInFuse(testFile);

    // Fuse getattr() will wait for file to be completed
    // when fuse release returns but does not finish
    String out = ShellUtils.execCommand("ls", "-sh", mMountPoint + testFile);
    Assert.assertFalse(out.isEmpty());
    Assert.assertEquals("40K", out.split("\\s+")[0]);

    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(testFile)));
    Assert.assertEquals(40 * Constants.KB,
        mFileSystem.getStatus(new AlluxioURI(testFile)).getLength());
  }

  @Test
  public void mkdirAndMv() throws Exception {
    String testFile = "/mvTestFile";
    String testFolder = "/mkdirTestFolder";
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);
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

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.AUTHENTICATION_INACTIVE_CHANNEL_REAUTHENTICATE_PERIOD, "250ms"})
  public void continueWithRevokedAuth() throws Exception {
    String testFile = "/tailTestFile";
    String content = "Alluxio Tail Test File Content";
    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("tail", "-c", "17", mMountPoint + testFile);
    Assert.assertEquals("Test File Content\n", result);

    /*
     * Sleeping will ensure that authentication sessions for existing clients held by FUSE will
     * expire on the server. This should have propagated back to the client and reauthentication
     * should happen on the background for making the second call succeed.
     *
     * Sleep more than authentication revocation timeout.
     */
    Thread.sleep(500);

    result = ShellUtils.execCommand("tail", "-c", "17", mMountPoint + testFile);
    Assert.assertEquals("Test File Content\n", result);
  }

  @Test
  public void touchAndLs() throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/lsTestFile", WritePType.MUST_CACHE, 10);
    String touchTestFile = "/touchTestFile";
    ShellUtils.execCommand("touch", mMountPoint + touchTestFile);

    String lsResult = ShellUtils.execCommand("ls", mMountPoint);
    Assert.assertTrue(lsResult.contains("lsTestFile"));
    Assert.assertTrue(lsResult.contains("touchTestFile"));
    Assert.assertTrue(mFileSystem.exists(new AlluxioURI(touchTestFile)));
  }

  /**
   * Create a file in Alluxio fuse mount point.
   *
   * @param filename the target file name
   */
  private void createFileInFuse(String filename) {
    try {
      ShellUtils.execCommand("dd", "if=/dev/zero",
          "of=" + mMountPoint + filename, "count=10", "bs=" + 4 * Constants.KB);
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
    File testFile = new File(mAlluxioCluster.getAlluxioHome() + path);
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(toWrite);
    fos.close();
    return testFile;
  }

  /**
   * Checks whether Alluxio-Fuse is mounted.
   *
   * @return true if fuse is mounted, false otherwise
   */
  protected boolean fuseMounted() throws IOException {
    String result = ShellUtils.execCommand("mount");
    return result.contains(mMountPoint);
  }

  /**
   * Waits for the Alluxio-Fuse to be mounted.
   *
   * @return true if Alluxio-Fuse mounted successfully in the given timeout, false otherwise
   */
  boolean waitForFuseMounted() throws IOException {
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
}
