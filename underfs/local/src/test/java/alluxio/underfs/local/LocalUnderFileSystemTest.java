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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the {@link LocalUnderFileSystem}.
 */
public class LocalUnderFileSystemTest {
  private String mLocalUfsRoot;
  private UnderFileSystem mLocalUfs;
  private static AlluxioConfiguration sConf =
      new InstancedConfiguration(ConfigurationUtils.defaults());

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() throws IOException {
    mLocalUfsRoot = mTemporaryFolder.getRoot().getAbsolutePath();
    mLocalUfs =
        UnderFileSystem.Factory.create(mLocalUfsRoot, UnderFileSystemConfiguration.defaults(sConf));
  }

  @Test
  public void exists() throws IOException {
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(filepath).close();

    assertTrue(mLocalUfs.isFile(filepath));

    mLocalUfs.deleteFile(filepath);

    assertFalse(mLocalUfs.isFile(filepath));
  }

  @Test
  public void create() throws IOException {
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    OutputStream os = mLocalUfs.create(filepath);
    os.close();

    assertTrue(mLocalUfs.isFile(filepath));

    File file = new File(filepath);
    assertTrue(file.exists());
  }

  @Test
  public void deleteFile() throws IOException {
    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(filepath).close();
    mLocalUfs.deleteFile(filepath);

    assertFalse(mLocalUfs.isFile(filepath));

    File file = new File(filepath);
    assertFalse(file.exists());
  }

  @Test
  public void recursiveDelete() throws IOException {
    String dirpath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath);
    String filepath = PathUtils.concatPath(dirpath, getUniqueFileName());
    mLocalUfs.create(filepath).close();
    mLocalUfs.deleteDirectory(dirpath, DeleteOptions.defaults().setRecursive(true));

    assertFalse(mLocalUfs.isDirectory(dirpath));

    File file = new File(filepath);
    assertFalse(file.exists());
  }

  @Test
  public void nonRecursiveDelete() throws IOException {
    String dirpath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath);
    String filepath = PathUtils.concatPath(dirpath, getUniqueFileName());
    mLocalUfs.create(filepath).close();
    mLocalUfs.deleteDirectory(dirpath, DeleteOptions.defaults().setRecursive(false));

    assertTrue(mLocalUfs.isDirectory(dirpath));

    File file = new File(filepath);
    assertTrue(file.exists());
  }

  @Test
  public void mkdirs() throws IOException {
    String parentPath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    String dirpath = PathUtils.concatPath(parentPath, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath);

    assertTrue(mLocalUfs.isDirectory(dirpath));

    File file = new File(dirpath);
    assertTrue(file.exists());
  }

  @Test
  public void mkdirsWithCreateParentEqualToFalse() throws IOException {
    String parentPath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    String dirpath = PathUtils.concatPath(parentPath, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath, MkdirsOptions.defaults(sConf).setCreateParent(false));

    assertFalse(mLocalUfs.isDirectory(dirpath));

    File file = new File(dirpath);
    assertFalse(file.exists());
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
    assertEquals(1, fileLocations.size());
    assertEquals(NetworkAddressUtils.getLocalHostName(
        (int) sConf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
        fileLocations.get(0));
  }

  @Test
  public void getOperationMode() throws IOException {
    Map<String, UfsMode> physicalUfsState = new Hashtable<>();
    // Check default
    Assert.assertEquals(UfsMode.READ_WRITE,
        mLocalUfs.getOperationMode(physicalUfsState));
    // Check NO_ACCESS mode
    physicalUfsState.put(AlluxioURI.SEPARATOR, UfsMode.NO_ACCESS);
    Assert.assertEquals(UfsMode.NO_ACCESS,
        mLocalUfs.getOperationMode(physicalUfsState));
  }

  @Test
  public void isFile() throws IOException {
    String dirpath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dirpath);
    assertFalse(mLocalUfs.isFile(dirpath));

    String filepath = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(filepath).close();
    assertTrue(mLocalUfs.isFile(filepath));
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

  @Test
  public void getDirStatusFails() throws IOException {
    mException.expect(IOException.class);
    String file = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(file).close();
    mLocalUfs.getDirectoryStatus(file);
  }

  @Test
  public void getDirStatus() throws IOException {
    String dir = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dir);
    UfsDirectoryStatus s = mLocalUfs.getDirectoryStatus(dir);
    assertTrue(s.isDirectory());
    assertFalse(s.isFile());
  }

  @Test
  public void getFileStatusFails() throws IOException {
    mException.expect(IOException.class);
    String dir = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.mkdirs(dir);
    mLocalUfs.getFileStatus(dir);
  }

  @Test
  public void getFileStatus() throws IOException {
    String file = PathUtils.concatPath(mLocalUfsRoot, getUniqueFileName());
    mLocalUfs.create(file).close();
    UfsFileStatus s = mLocalUfs.getFileStatus(file);
    assertFalse(s.isDirectory());
    assertTrue(s.isFile());
  }

  @Test
  public void testBrokenSymlinkSkip() throws IOException {
    InstancedConfiguration c = new InstancedConfiguration(sConf.copyProperties());
    c.set(PropertyKey.UNDERFS_LOCAL_SKIP_BROKEN_SYMLINKS, true);
    mLocalUfs =
        UnderFileSystem.Factory.create(mLocalUfsRoot, UnderFileSystemConfiguration.defaults(c));
    Path linkPath = createNonExistentSymlink();
    assertTrue(Files.exists(linkPath, LinkOption.NOFOLLOW_LINKS));
    assertFalse(Files.exists(linkPath));
    UfsStatus[] statuses = mLocalUfs.listStatus(mLocalUfsRoot);
    assertNotNull(statuses);
    assertEquals(0, statuses.length);
  }

  @Test
  public void testSymlinkNonSkip() throws IOException {
    InstancedConfiguration c = new InstancedConfiguration(sConf.copyProperties());
    c.set(PropertyKey.UNDERFS_LOCAL_SKIP_BROKEN_SYMLINKS, false);
    mLocalUfs =
        UnderFileSystem.Factory.create(mLocalUfsRoot, UnderFileSystemConfiguration.defaults(c));
    Path linkPath = createNonExistentSymlink();
    assertTrue(Files.exists(linkPath, LinkOption.NOFOLLOW_LINKS));
    assertFalse(Files.exists(linkPath));
    assertThrows(NoSuchFileException.class, () -> mLocalUfs.listStatus(mLocalUfsRoot));
  }

  private Path createNonExistentSymlink() throws IOException {

    Path linkPath = Paths.get(mLocalUfsRoot, "test");
    return Files.createSymbolicLink(linkPath,
        Paths.get("/tmp/nonexiststent/alluxio/file"));
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
