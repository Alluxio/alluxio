/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.shell;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.AlluxioURI;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.master.LocalAlluxioCluster;

/**
 * Unit tests on alluxio.command.Utils.
 *
 * Note that the test case for {@link AlluxioShellUtils#validatePath(String, Configuration)} is
 * already covered in {@link AlluxioShellUtils#getFilePath(String, Configuration)}. Hence only
 * getFilePathTest is specified.
 */
public final class AlluxioShellUtilsTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource();
  private FileSystem mFileSystem = null;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @Test
  public void getFilePathTest() throws IOException {
    String[] paths =
        new String[] {Constants.HEADER + "localhost:19998/dir",
            Constants.HEADER_FT + "localhost:19998/dir", "/dir", "dir"};
    String expected = "/dir";
    for (String path : paths) {
      String result = AlluxioShellUtils.getFilePath(path, new Configuration());
      Assert.assertEquals(expected, result);
    }
  }

  public enum FsType {
    TFS, LOCAL
  }

  public String resetTachyonFileHierarchy() throws IOException, AlluxioException {
    return resetTachyonFileHierarchy(mFileSystem);
  }

  public static String resetTachyonFileHierarchy(FileSystem fs)
      throws IOException, AlluxioException {
    /**
     * Generate such local structure /testWildCards
     *                                ├── foo |
     *                                        ├── foobar1
     *                                        └── foobar2
     *                                ├── bar |
     *                                        └── foobar3
     *                                └── foobar4
     */
    if (fs.exists(new AlluxioURI("/testWildCards"))) {
      fs.delete(new AlluxioURI("/testWildCards"));
    }
    fs.createDirectory(new AlluxioURI("/testWildCards"));
    fs.createDirectory(new AlluxioURI("/testWildCards/foo"));
    fs.createDirectory(new AlluxioURI("/testWildCards/bar"));

    FileSystemTestUtils.createByteFile(fs, "/testWildCards/foo/foobar1", WriteType.MUST_CACHE, 10);
    FileSystemTestUtils.createByteFile(fs, "/testWildCards/foo/foobar2", WriteType.MUST_CACHE, 20);
    FileSystemTestUtils.createByteFile(fs, "/testWildCards/bar/foobar3", WriteType.MUST_CACHE, 30);
    FileSystemTestUtils.createByteFile(fs, "/testWildCards/foobar4", WriteType.MUST_CACHE, 40);
    return "/testWildCards";
  }

  public String resetLocalFileHierarchy() throws IOException {
    return resetLocalFileHierarchy(mLocalAlluxioClusterResource.get());
  }

  public static String resetLocalFileHierarchy(LocalAlluxioCluster localTachyonCluster)
      throws IOException {
    /**
     * Generate such local structure /testWildCards
     *                                ├── foo |
     *                                        ├── foobar1
     *                                        └── foobar2
     *                                ├── bar |
     *                                        └── foobar3
     *                                └── foobar4
     */
    FileUtils.deleteDirectory(new File(localTachyonCluster.getTachyonHome() + "/testWildCards"));
    new File(localTachyonCluster.getTachyonHome() + "/testWildCards").mkdir();
    new File(localTachyonCluster.getTachyonHome() + "/testWildCards/foo").mkdir();
    new File(localTachyonCluster.getTachyonHome() + "/testWildCards/bar").mkdir();

    new File(localTachyonCluster.getTachyonHome() + "/testWildCards/foo/foobar1").createNewFile();
    new File(localTachyonCluster.getTachyonHome() + "/testWildCards/foo/foobar2").createNewFile();
    new File(localTachyonCluster.getTachyonHome() + "/testWildCards/bar/foobar3").createNewFile();
    new File(localTachyonCluster.getTachyonHome() + "/testWildCards/foobar4").createNewFile();

    return localTachyonCluster.getTachyonHome() + "/testWildCards";
  }

  public List<String> getPaths(String path, FsType fsType) throws IOException, TException {
    List<String> ret = null;
    if (fsType == FsType.TFS) {
      List<AlluxioURI> tPaths = AlluxioShellUtils.getTachyonURIs(mFileSystem, new AlluxioURI(path));
      ret = new ArrayList<String>(tPaths.size());
      for (AlluxioURI tPath : tPaths) {
        ret.add(tPath.getPath());
      }
    } else if (fsType == FsType.LOCAL) {
      List<File> tPaths = AlluxioShellUtils.getFiles(path);
      ret = new ArrayList<String>(tPaths.size());
      for (File tPath : tPaths) {
        ret.add(tPath.getPath());
      }
    }
    Collections.sort(ret);
    return ret;
  }

  public String resetFsHierarchy(FsType fsType) throws IOException, AlluxioException {
    if (fsType == FsType.TFS) {
      return resetTachyonFileHierarchy();
    } else if (fsType == FsType.LOCAL) {
      return resetLocalFileHierarchy();
    } else {
      return null;
    }
  }

  @Test
  public void getPathTest() throws IOException, AlluxioException, TException {
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
  public void matchTest() {
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
