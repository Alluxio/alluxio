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

package alluxio.underfs.local;

import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Unit tests for the {@link LocalUnderFileSystem}.
 */
public class LocalUnderFileSystemTest {
  private String mLocalUfsRoot;
  private UnderFileSystem mLocalUfs;
  private final File mTestDirectory = createTestingDirectory();
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Before
  public void setUp() throws IOException {
    mLocalUfsRoot = mTestDirectory.getAbsolutePath();
    mLocalUfs = LocalUnderFileSystem.get(mLocalUfsRoot);
  }

  @After
  public void tearDown() {
    cleanUpOldFiles(mTestDirectory);
  }

  @Test
  public void create() throws IOException {
    final int size = 512;
    final String filepath = createFileWithRandomName(getRandomBytes(size));
    Assert.assertTrue(mLocalUfs.exists(filepath));
    Assert.assertEquals(size, mLocalUfs.getFileSize(filepath));
  }

  @Test
  public void delete() throws IOException {
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(filepath);
    mLocalUfs.delete(filepath, true);
    Assert.assertFalse(mLocalUfs.exists(filepath));
  }

  @Test
  public void recursiveDelete() throws IOException {
    String dirpath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath, true);
    String filepath = PathUtils.concatPath(dirpath, getUniqueFileName());
    mLocalUfs.create(filepath);
    mLocalUfs.delete(dirpath, true);
    Assert.assertFalse(mLocalUfs.exists(dirpath));
  }

  @Test
  public void mkdirs() throws IOException {
    String parentPath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    String dirpath = PathUtils.concatPath(parentPath, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath, true);
    Assert.assertTrue(mLocalUfs.exists(dirpath));
  }

  @Test
  public void open() throws IOException {
    final int size = 512;
    final byte[] bytes = getRandomBytes(size);
    final String filepath = createFileWithRandomName(bytes);
    InputStream is = mLocalUfs.open(filepath);
    byte[] bytes1 = new byte[size];
    is.read(bytes1);
    is.close();
    Assert.assertArrayEquals(bytes, bytes1);
  }

  @Test
  public void getFileLocations() throws IOException {
    final int size = 512;
    final byte[] bytes = getRandomBytes(size);
    final String filepath = createFileWithRandomName(bytes);
    List<String> fileLocations = mLocalUfs.getFileLocations(filepath);
    Assert.assertEquals(1, fileLocations.size());
    Assert.assertEquals(NetworkAddressUtils.getLocalHostName(), fileLocations.get(0));
  }

  @Test
  public void isFile() throws IOException {
    final String dirpath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath, true);
    Assert.assertFalse(mLocalUfs.isFile(dirpath));

    final String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(filepath);
    Assert.assertTrue(mLocalUfs.isFile(filepath));
  }

  @Test
  public void rename() throws IOException {
    final int size = 512;
    final byte[] bytes = getRandomBytes(size);
    final String filepath1 = createFileWithRandomName(bytes);
    final String filepath2 = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.rename(filepath1, filepath2);
    InputStream is = mLocalUfs.open(filepath2);
    byte[] bytes1 = new byte[size];
    is.read(bytes1);
    is.close();
    Assert.assertArrayEquals(bytes, bytes1);
  }

  private byte[] getRandomBytes(final int size) {
    String s = RandomStringUtils.randomAlphanumeric(size);
    return s.getBytes();
  }

  private String createFileWithRandomName(final byte[] bytes) throws IOException {
    final String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    OutputStream os = mLocalUfs.create(filepath);
    os.write(bytes);
    os.close();
    return filepath;
  }

  private String getUniqueFileName() {
    long time = System.nanoTime();
    String fileName = "" + time;
    return fileName;
  }

  private File createTestingDirectory() {
    final File tmpDir = new File(System.getProperty("java.io.tmpdir"), "alluxio-tests");
    if (tmpDir.exists()) {
      cleanUpOldFiles(tmpDir);
    }
    if (!tmpDir.exists()) {
      if (!tmpDir.mkdir()) {
        throw new RuntimeException(
                "Failed to create testing directory " + tmpDir.getAbsolutePath());
      }
    }
    return tmpDir;
  }

  private void cleanUpOldFiles(File dir) {
    File[] files = dir.listFiles();
    for (File file : files) {
      try {
        alluxio.util.io.FileUtils.deletePathRecursively(file.getAbsolutePath());
      } catch (Exception e) {
        LOG.warn("Failed to delete {}", file.getAbsolutePath(), e);
      }
    }
  }
}
