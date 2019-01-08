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
import alluxio.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.fuse.AlluxioFuseFileSystem;
import alluxio.fuse.AlluxioFuseOptions;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;
import alluxio.util.WaitForOptions;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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
 */
public class FuseFileSystemIntegrationTest {
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;
  private static final int BLOCK_SIZE = 4 * Constants.KB;

  // Fuse user group translation needs to be enabled to support chown/chgrp/ls commands
  // to show accurate information
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.FUSE_USER_GROUP_TRANSLATION_ENABLED, true)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE)
          .build();

  private static String sAlluxioRoot;
  private static boolean sFuseInstalled;
  private static FileSystem sFileSystem;
  private static AlluxioFuseFileSystem sFuseFileSystem;
  private static Thread sFuseThread;
  private static String sMountPoint;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // This test only runs when fuse is installed
    sFuseInstalled = AlluxioFuseUtils.isFuseInstalled();
    Assume.assumeTrue(sFuseInstalled);

    sFileSystem = sLocalAlluxioClusterResource.get().getClient();

    sMountPoint = AlluxioTestDirectory
        .createTemporaryDirectory("FuseMountPoint").getAbsolutePath();
    sAlluxioRoot = "/";

    AlluxioFuseOptions options = new AlluxioFuseOptions(sMountPoint,
        sAlluxioRoot, false, new ArrayList<>());
    sFuseFileSystem = new AlluxioFuseFileSystem(sFileSystem, options);
    sFuseThread = new Thread(() -> sFuseFileSystem.mount(Paths.get(sMountPoint),
        true, false, new String[]{"-odirect_io"}));
    sFuseThread.start();

    if (!waitForFuseMounted()) {
      // Fuse may not be mounted within timeout and we need to umount it
      umountFuse();
      Assume.assumeTrue(false);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sFuseInstalled && fuseMounted()) {
      umountFuse();
    }
  }

  @After
  public void after() throws Exception {
    for (URIStatus status : sFileSystem.listStatus(new AlluxioURI(sAlluxioRoot))) {
      DeletePOptions options = DeletePOptions.newBuilder().setRecursive(true).build();
      sFileSystem.delete(new AlluxioURI(status.getPath()), options);
    }
  }

  @Test
  public void cat() throws Exception {
    String testFile = "/catTestFile";
    String content = "Alluxio Cat Test File Content";

    try (FileOutStream os = sFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }

    String result = ShellUtils.execCommand("cat", sMountPoint + testFile);
    Assert.assertEquals(content + "\n", result);
  }

  @Test
  public void chgrp() throws Exception {
    String testFile = "/chgrpTestFile";
    String userName = System.getProperty("user.name");
    String groupName = AlluxioFuseUtils.getGroupName(userName);
    FileSystemTestUtils.createByteFile(sFileSystem, testFile, WritePType.WRITE_MUST_CACHE, 10);
    ShellUtils.execCommand("chgrp", groupName, sMountPoint + testFile);
    Assert.assertEquals(groupName, sFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
  }

  @Test
  public void chmod() throws Exception {
    String testFile = "/chmodTestFile";
    FileSystemTestUtils.createByteFile(sFileSystem, testFile, WritePType.WRITE_MUST_CACHE, 10);
    ShellUtils.execCommand("chmod", "777", sMountPoint + testFile);
    Assert.assertEquals((short) 0777, sFileSystem.getStatus(new AlluxioURI(testFile)).getMode());
  }

  @Test
  public void chown() throws Exception {
    String testFile = "/chownTestFile";
    FileSystemTestUtils.createByteFile(sFileSystem, testFile, WritePType.WRITE_MUST_CACHE, 10);

    String userName = System.getProperty("user.name");
    String groupName = AlluxioFuseUtils.getGroupName(userName);
    ShellUtils.execCommand("chown", userName + ":" + groupName, sMountPoint + testFile);
    Assert.assertEquals(userName, sFileSystem.getStatus(new AlluxioURI(testFile)).getOwner());
    Assert.assertEquals(groupName, sFileSystem.getStatus(new AlluxioURI(testFile)).getGroup());
  }

  @Test
  public void cp() throws Exception {
    String testFile = "/cpTestFile";
    String content = "Alluxio Cp Test File Content";
    File localFile = generateFileContent("/TestFileOnLocalPath", content.getBytes());

    ShellUtils.execCommand("cp", localFile.getPath(), sMountPoint + testFile);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testFile)));

    // Fuse release() is async
    // Cp again to make sure the first cp is completed
    String testFolder = "/cpTestFolder";
    ShellUtils.execCommand("mkdir", sMountPoint + testFolder);
    ShellUtils.execCommand("cp", sMountPoint + testFile, sMountPoint + testFolder + testFile);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testFolder + testFile)));

    byte[] read = new byte[content.length()];
    try (FileInStream is = sFileSystem.openFile(new AlluxioURI(testFile),
        OpenFilePOptions.newBuilder().setReadType(ReadPType.READ_NO_CACHE).build())) {
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
    ShellUtils.execCommand("head", "-c", "10", sMountPoint + testFile);

    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testFile)));
    Assert.assertEquals(40 * Constants.KB,
        sFileSystem.getStatus(new AlluxioURI(testFile)).getLength());

    String output = ShellUtils.execCommand("du", "-k", sMountPoint + testFile);
    Assert.assertEquals("40", output.split("\\s+")[0]);

    ShellUtils.execCommand("rm", sMountPoint + testFile);
    Assert.assertFalse(sFileSystem.exists(new AlluxioURI(testFile)));
  }

  @Test
  public void head() throws Exception {
    String testFile = "/headTestFile";
    String content = "Alluxio Head Test File Content";
    try (FileOutStream os = sFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("head", "-c", "17", sMountPoint + testFile);
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
    String out = ShellUtils.execCommand("ls", "-sh", sMountPoint + testFile);
    Assert.assertFalse(out.isEmpty());
    Assert.assertEquals("40K", out.split("\\s+")[0]);

    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testFile)));
    Assert.assertEquals(40 * Constants.KB,
        sFileSystem.getStatus(new AlluxioURI(testFile)).getLength());
  }

  @Test
  public void mkdirAndMv() throws Exception {
    String testFile = "/mvTestFile";
    String testFolder = "/mkdirTestFolder";
    FileSystemTestUtils.createByteFile(sFileSystem, testFile, WritePType.WRITE_MUST_CACHE, 10);
    ShellUtils.execCommand("mkdir", sMountPoint + testFolder);
    ShellUtils.execCommand("mv", sMountPoint + testFile,
        sMountPoint + testFolder + testFile);
    Assert.assertFalse(sFileSystem.exists(new AlluxioURI(testFile)));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(testFolder + testFile)));
  }

  @Test
  public void tail() throws Exception {
    String testFile = "/tailTestFile";
    String content = "Alluxio Tail Test File Content";
    try (FileOutStream os = sFileSystem.createFile(new AlluxioURI(testFile))) {
      os.write(content.getBytes());
    }
    String result = ShellUtils.execCommand("tail", "-c", "17", sMountPoint + testFile);
    Assert.assertEquals("Test File Content\n", result);
  }

  @Test
  public void touchAndLs() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/lsTestFile", WritePType.WRITE_MUST_CACHE, 10);
    String touchTestFile = "/touchTestFile";
    ShellUtils.execCommand("touch", sMountPoint + touchTestFile);

    String lsResult = ShellUtils.execCommand("ls", sMountPoint);
    Assert.assertTrue(lsResult.contains("lsTestFile"));
    Assert.assertTrue(lsResult.contains("touchTestFile"));
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI(touchTestFile)));
  }

  /**
   * Create a file in Alluxio fuse mount point.
   *
   * @param filename the target file name
   */
  private void createFileInFuse(String filename) {
    try {
      ShellUtils.execCommand("dd", "if=/dev/zero",
          "of=" + sMountPoint + filename, "count=10", "bs=" + 4 * Constants.KB);
    } catch (IOException e) {
      fail();
    }
  }

  /**
   * Umounts the Alluxio-Fuse.
   */
  private static void umountFuse() throws InterruptedException {
    sFuseFileSystem.umount();
    sFuseThread.interrupt();
    sFuseThread.join();
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
    File testFile = new File(sLocalAlluxioClusterResource.get().getAlluxioHome() + path);
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
  private static boolean fuseMounted() throws IOException {
    String result = ShellUtils.execCommand("mount");
    return result.contains(sMountPoint);
  }

  /**
   * Waits for the Alluxio-Fuse to be mounted.
   *
   * @return true if Alluxio-Fuse mounted successfully in the given timeout, false otherwise
   */
  private static boolean waitForFuseMounted() throws IOException {
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
