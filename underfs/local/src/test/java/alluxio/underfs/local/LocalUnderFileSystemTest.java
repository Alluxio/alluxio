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

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    mLocalUfsRoot = mTemporaryFolder.getRoot().getAbsolutePath();
    mLocalUfs = UnderFileSystem.Factory.create(mLocalUfsRoot);
  }

  @Test
  public void exists() throws IOException {
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(filepath).close();

    Assert.assertTrue(mLocalUfs.isFile(filepath));

    mLocalUfs.deleteFile(filepath);

    Assert.assertFalse(mLocalUfs.isFile(filepath));
  }

  @Test
  public void create() throws IOException {
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    OutputStream os = mLocalUfs.create(filepath);

    Assert.assertFalse(mLocalUfs.isFile(filepath));

    os.close();

    Assert.assertTrue(mLocalUfs.isFile(filepath));

    File file = new File(filepath);
    Assert.assertTrue(file.exists());
  }

  @Test
  public void deleteFile() throws IOException {
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(filepath).close();
    mLocalUfs.deleteFile(filepath);

    Assert.assertFalse(mLocalUfs.isFile(filepath));

    File file = new File(filepath);
    Assert.assertFalse(file.exists());
  }

  @Test
  public void recursiveDelete() throws IOException {
    String dirpath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath);
    String filepath = PathUtils.concatPath(dirpath, getUniqueFileName());
    mLocalUfs.create(filepath).close();
    mLocalUfs.deleteDirectory(dirpath, DeleteOptions.defaults().setRecursive(true));

    Assert.assertFalse(mLocalUfs.isDirectory(dirpath));

    File file = new File(filepath);
    Assert.assertFalse(file.exists());
  }

  @Test
  public void nonRecursiveDelete() throws IOException {
    String dirpath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath);
    String filepath = PathUtils.concatPath(dirpath, getUniqueFileName());
    mLocalUfs.create(filepath).close();
    mLocalUfs.deleteDirectory(dirpath, DeleteOptions.defaults().setRecursive(false));

    Assert.assertTrue(mLocalUfs.isDirectory(dirpath));

    File file = new File(filepath);
    Assert.assertTrue(file.exists());
  }

  @Test
  public void mkdirs() throws IOException {
    String parentPath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    String dirpath = PathUtils.concatPath(parentPath, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath);

    Assert.assertTrue(mLocalUfs.isDirectory(dirpath));

    File file = new File(dirpath);
    Assert.assertTrue(file.exists());
  }

  @Test
  public void mkdirsWithCreateParentEqualToFalse() throws IOException {
    String parentPath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    String dirpath = PathUtils.concatPath(parentPath, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath, MkdirsOptions.defaults().setCreateParent(false));

    Assert.assertFalse(mLocalUfs.isDirectory(dirpath));

    File file = new File(dirpath);
    Assert.assertFalse(file.exists());
  }

  @Test
  public void open() throws IOException {
    byte[] bytes = getBytes();
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());

    OutputStream os = mLocalUfs.create(filepath);
    os.write(bytes);
    os.close();

    InputStream is = mLocalUfs.open(filepath);
    byte[] bytes1 = new byte[bytes.length];
    is.read(bytes1);
    is.close();

    Assert.assertArrayEquals(bytes, bytes1);
  }

  @Test
  public void getFileLocations() throws IOException {
    byte[] bytes = getBytes();
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());

    OutputStream os = mLocalUfs.create(filepath);
    os.write(bytes);
    os.close();

    List<String> fileLocations = mLocalUfs.getFileLocations(filepath);
    Assert.assertEquals(1, fileLocations.size());
    Assert.assertEquals(NetworkAddressUtils.getLocalHostName(), fileLocations.get(0));
  }

  @Test
  public void isFile() throws IOException {
    String dirpath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath);
    Assert.assertFalse(mLocalUfs.isFile(dirpath));

    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(filepath).close();
    Assert.assertTrue(mLocalUfs.isFile(filepath));
  }

  @Test
  public void renameFile() throws IOException {
    byte[] bytes = getBytes();
    String filepath1 = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());

    OutputStream os = mLocalUfs.create(filepath1);
    os.write(bytes);
    os.close();

    String filepath2 = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.renameFile(filepath1, filepath2);

    InputStream is = mLocalUfs.open(filepath2);
    byte[] bytes1 = new byte[bytes.length];
    is.read(bytes1);
    is.close();

    Assert.assertArrayEquals(bytes, bytes1);
  }

  private byte[] getBytes() {
    String s = "BYTES";
    return s.getBytes();
  }

  private String getUniqueFileName() {
    long time = System.nanoTime();
    String fileName = "" + time;
    return fileName;
  }
}
