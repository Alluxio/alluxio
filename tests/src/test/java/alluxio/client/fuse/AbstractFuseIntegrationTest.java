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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
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
 * This is the base class of Fuse tests. It describes the POSIX API functionalities that all Fuse
 * implementations and launch ways should support. Each Fuse implementation and launch ways is
 * expected to create a test that extends this base class.
 */
public abstract class AbstractFuseIntegrationTest {
  protected static final String ALLUXIO_ROOT = "/";
  private static final int BLOCK_SIZE = 4 * Constants.KB;
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;

  private final LocalAlluxioCluster mAlluxioCluster = new LocalAlluxioCluster();
  private FileSystem mFileSystem;
  protected String mMountPoint;

  @Rule
  public TestName mTestName = new TestName();

  /**
   * Overwrites the test configuration in this method.
   */
  public abstract void configure();

  /**
   * Mounts the Fuse application if needed.
   *
   * @param fileSystem the filesystem to create the Fuse application
   * @param mountPoint the Fuse mount point
   * @param alluxioRoot the Fuse mounted alluxio root
   */
  public abstract void mountFuse(FileSystem fileSystem, String mountPoint, String alluxioRoot);

  /**
   * Umounts the given fuse mount point.
   *
   * @param mountPoint the Fuse mount point
   */
  public abstract void umountFuse(String mountPoint) throws Exception;

  @BeforeClass
  public static void beforeClass() {
    assumeTrue("This test only runs when libfuse is installed", AlluxioFuseUtils.isFuseInstalled());
  }

  @Before
  public void before() throws Exception {
    String clusterName =
        IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName());
    mMountPoint = AlluxioTestDirectory.createTemporaryDirectory(clusterName).getAbsolutePath();
    mAlluxioCluster.initConfiguration(ALLUXIO_ROOT);
    ServerConfiguration.set(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED, true);
    ServerConfiguration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);
    configure();
    IntegrationTestUtils.reserveMasterPorts();
    ServerConfiguration.global().validate();
    mAlluxioCluster.start();
    mFileSystem = mAlluxioCluster.getClient();
    mountFuse(mFileSystem, mMountPoint, ALLUXIO_ROOT);
    if (!waitForFuseMounted()) {
      stop();
      fail("Could not setup FUSE mount point");
    }
  }

  @After
  public void after() throws Exception {
    stop();
  }

  private void stop() throws Exception {
    try {
      mAlluxioCluster.stop();
    } finally {
      IntegrationTestUtils.releaseMasterPorts();
    }
    if (fuseMounted()) {
      try {
        umountFuse(mMountPoint);
      } catch (Exception e) {
        // The Fuse application may be unmounted by the cluster stop
      }
      if (fuseMounted()) {
        ShellUtils.execCommand("umount", mMountPoint);
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
    assertEquals(content + "\n", result);
  }

  @Test
  public void chgrp() throws Exception {
    String testFile = "/chgrpTestFile";
    String userName = System.getProperty("user.name");
    String groupName = AlluxioFuseUtils.getGroupName(userName);
    FileSystemTestUtils.createByteFile(mFileSystem, testFile, WritePType.MUST_CACHE, 10);
    ShellUtils.execCommand("chgrp", groupName, mMountPoint + testFile);
    assertEquals(groupName, mFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
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
    String groupName = AlluxioFuseUtils.getGroupName(userName);
    ShellUtils.execCommand("chown", userName + ":" + groupName, mMountPoint + testFile);
    assertEquals(userName, mFileSystem.getStatus(new AlluxioURI(testFile)).getOwner());
    assertEquals(groupName, mFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
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
    assertEquals(content, new String(read, "UTF8"));
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
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.AUTHENTICATION_INACTIVE_CHANNEL_REAUTHENTICATE_PERIOD, "250ms"})
  public void continueWithRevokedAuth() throws Exception {
    String testFile = "/tailTestFile";
    String content = "Alluxio Tail Test File Content";
    try (FileOutStream os = mFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("tail", "-c", "17", mMountPoint + testFile);
    assertEquals("Test File Content\n", result);

    /*
     * Sleeping will ensure that authentication sessions for existing clients held by FUSE will
     * expire on the server. This should have propagated back to the client and reauthentication
     * should happen on the background for making the second call succeed.
     *
     * Sleep more than authentication revocation timeout.
     */
    Thread.sleep(500);

    result = ShellUtils.execCommand("tail", "-c", "17", mMountPoint + testFile);
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
    File testFile = new File(mAlluxioCluster.getAlluxioHome() + path);
    testFile.createNewFile();
    try (FileOutputStream fos = new FileOutputStream(testFile)) {
      fos.write(toWrite);
    }
    return testFile;
  }

  /**
   * Checks whether Alluxio-Fuse is mounted.
   *
   * @return true if fuse is mounted, false otherwise
   */
  private boolean fuseMounted() throws IOException {
    String result = ShellUtils.execCommand("mount");
    return result.contains(mMountPoint);
  }

  /**
   * Waits for the Alluxio-Fuse to be mounted.
   *
   * @return true if Alluxio-Fuse mounted successfully in the given timeout, false otherwise
   */
  boolean waitForFuseMounted() {
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
