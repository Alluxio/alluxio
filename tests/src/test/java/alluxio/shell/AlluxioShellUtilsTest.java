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

package alluxio.shell;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.DeleteOptions;
import alluxio.exception.AlluxioException;
import alluxio.master.LocalAlluxioCluster;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests on {@link alluxio.shell.AlluxioShellUtils}.
 *
 * Note that the test case for {@link AlluxioShellUtils#validatePath(String)} is already covered
 * in {@link AlluxioShellUtils#getFilePath(String)}. Hence only getFilePathTest is specified.
 */
public final class AlluxioShellUtilsTest {
  public static final String TEST_DIR = "/testDir";

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();
  private FileSystem mFileSystem = null;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @Test
  public void getFilePath() throws IOException {
    String[] paths =
        new String[] {Constants.HEADER + "localhost:19998/dir",
            Constants.HEADER_FT + "localhost:19998/dir", "/dir", "dir"};
    String expected = "/dir";
    for (String path : paths) {
      String result = AlluxioShellUtils.getFilePath(path);
      Assert.assertEquals(expected, result);
    }
  }

  public enum FsType {
    TFS, LOCAL
  }

  public String resetFileHierarchy() throws IOException, AlluxioException {
    return resetFileHierarchy(mFileSystem);
  }

  public static String resetFileHierarchy(FileSystem fs)
      throws IOException, AlluxioException {
    /**
     * Generate such local structure TEST_DIR
     *                                ├── foo |
     *                                        ├── foobar1
     *                                        └── foobar2
     *                                ├── bar |
     *                                        └── foobar3
     *                                └── foobar4
     */
    if (fs.exists(new AlluxioURI(TEST_DIR))) {
      fs.delete(new AlluxioURI(TEST_DIR), DeleteOptions.defaults().setRecursive(true));
    }
    fs.createDirectory(new AlluxioURI(TEST_DIR));
    fs.createDirectory(new AlluxioURI(TEST_DIR + "/foo"));
    fs.createDirectory(new AlluxioURI(TEST_DIR + "/bar"));

    FileSystemTestUtils.createByteFile(fs, TEST_DIR + "/foo/foobar1", WriteType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(fs, TEST_DIR + "/foo/foobar2", WriteType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(fs, TEST_DIR + "/bar/foobar3", WriteType.MUST_CACHE, 30);
    FileSystemTestUtils.createByteFile(fs, TEST_DIR + "/foobar4", WriteType.MUST_CACHE, 40);
    return TEST_DIR;
  }

  public String resetLocalFileHierarchy() throws IOException {
    return resetLocalFileHierarchy(mLocalAlluxioClusterResource.get());
  }

  public static String resetLocalFileHierarchy(LocalAlluxioCluster localAlluxioCluster)
      throws IOException {
    /**
     * Generate such local structure TEST_DIR
     *                                ├── foo |
     *                                        ├── foobar1
     *                                        └── foobar2
     *                                ├── bar |
     *                                        └── foobar3
     *                                └── foobar4
     */
    FileUtils.deleteDirectory(new File(localAlluxioCluster.getAlluxioHome() + TEST_DIR));
    new File(localAlluxioCluster.getAlluxioHome() + TEST_DIR).mkdir();
    new File(localAlluxioCluster.getAlluxioHome() + TEST_DIR + "/foo").mkdir();
    new File(localAlluxioCluster.getAlluxioHome() + TEST_DIR + "/bar").mkdir();

    new File(localAlluxioCluster.getAlluxioHome() + TEST_DIR + "/foo/foobar1").createNewFile();
    new File(localAlluxioCluster.getAlluxioHome() + TEST_DIR + "/foo/foobar2").createNewFile();
    new File(localAlluxioCluster.getAlluxioHome() + TEST_DIR + "/bar/foobar3").createNewFile();
    new File(localAlluxioCluster.getAlluxioHome() + TEST_DIR + "/foobar4").createNewFile();

    return localAlluxioCluster.getAlluxioHome() + TEST_DIR;
  }

  public List<String> getPaths(String path, FsType fsType) throws IOException, TException {
    List<String> ret = null;
    if (fsType == FsType.TFS) {
      List<AlluxioURI> tPaths = AlluxioShellUtils.getAlluxioURIs(mFileSystem, new AlluxioURI(path));
      ret = new ArrayList<>(tPaths.size());
      for (AlluxioURI tPath : tPaths) {
        ret.add(tPath.getPath());
      }
    } else if (fsType == FsType.LOCAL) {
      List<File> tPaths = AlluxioShellUtils.getFiles(path);
      ret = new ArrayList<>(tPaths.size());
      for (File tPath : tPaths) {
        ret.add(tPath.getPath());
      }
    }
    Collections.sort(ret);
    return ret;
  }

  public String resetFsHierarchy(FsType fsType) throws IOException, AlluxioException {
    if (fsType == FsType.TFS) {
      return resetFileHierarchy();
    } else if (fsType == FsType.LOCAL) {
      return resetLocalFileHierarchy();
    } else {
      return null;
    }
  }

  @Test
  public void getPath() throws IOException, AlluxioException, TException {
    for (FsType fsType : FsType.values()) {
      String rootDir = resetFsHierarchy(fsType);

      List<String> tl1 = getPaths(rootDir + "/foo", fsType);
      Assert.assertEquals(tl1.size(), 1);
      Assert.assertEquals(tl1.get(0), rootDir + "/foo");

      // Trailing slash
      List<String> tl2 = getPaths(rootDir + "/foo/", fsType);
      Assert.assertEquals(tl2.size(), 1);
      Assert.assertEquals(tl2.get(0), rootDir + "/foo");

      // Wildcard
      List<String> tl3 = getPaths(rootDir + "/foo/*", fsType);
      Assert.assertEquals(tl3.size(), 2);
      Assert.assertEquals(tl3.get(0), rootDir + "/foo/foobar1");
      Assert.assertEquals(tl3.get(1), rootDir + "/foo/foobar2");

      // Trailing slash + wildcard
      List<String> tl4 = getPaths(rootDir + "/foo/*/", fsType);
      Assert.assertEquals(tl4.size(), 2);
      Assert.assertEquals(tl4.get(0), rootDir + "/foo/foobar1");
      Assert.assertEquals(tl4.get(1), rootDir + "/foo/foobar2");

      // Multiple wildcards
      List<String> tl5 = getPaths(rootDir + "/*/foo*", fsType);
      Assert.assertEquals(tl5.size(), 3);
      Assert.assertEquals(tl5.get(0), rootDir + "/bar/foobar3");
      Assert.assertEquals(tl5.get(1), rootDir + "/foo/foobar1");
      Assert.assertEquals(tl5.get(2), rootDir + "/foo/foobar2");
    }
  }

  @Test
  public void match() {
    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c", "/a/*"), true);
    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c", "/a/*/"), true);
    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c", "/a/*/c"), true);
    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c", "/a/*/*"), true);
    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c", "/a/*/*/"), true);
    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c/", "/a/*/*/"), true);
    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c/", "/a/*/*"), true);

    Assert.assertEquals(AlluxioShellUtils.match("/foo/bar/foobar/", "/foo*/*"), true);
    Assert.assertEquals(AlluxioShellUtils.match("/foo/bar/foobar/", "/*/*/foobar"), true);

    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c/", "/b/*"), false);
    Assert.assertEquals(AlluxioShellUtils.match("/", "/*/*"), false);

    Assert.assertEquals(AlluxioShellUtils.match("/a/b/c", "*"), true);
    Assert.assertEquals(AlluxioShellUtils.match("/", "/*"), true);
  }
}
