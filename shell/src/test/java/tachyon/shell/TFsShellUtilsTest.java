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

package tachyon.shell;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;

/**
 * Unit tests on tachyon.command.Utils.
 *
 * Note that the test case for validatePath() is already covered in getFilePath. Hence only
 * getFilePathTest is specified.
 */
public class TFsShellUtilsTest {
  private static final int SIZE_BYTES = Constants.MB * 10;
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(SIZE_BYTES, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @Test
  public void getFilePathTest() throws IOException {
    String[] paths =
        new String[] {Constants.HEADER + "localhost:19998/dir",
            Constants.HEADER_FT + "localhost:19998/dir", "/dir", "dir"};
    String expected = "/dir";
    for (String path : paths) {
      String result = TFsShellUtils.getFilePath(path, new TachyonConf());
      Assert.assertEquals(expected, result);
    }
  }

  public enum FsType {
    TFS,
    LOCAL
  }

  public static Comparator<File> createFilePathComparator() {
    return  new Comparator<File>() {
      public int compare(File file1, File file2) {
        // ascending order
        return file1.getAbsoluteFile().compareTo(file2.getAbsoluteFile());
      }
    };
  }

  public static Comparator<TachyonURI> createTachyonURIComparator() {
    return  new Comparator<TachyonURI>() {
      public int compare(TachyonURI tUri1, TachyonURI tUri2) {
        // ascending order
        return tUri1.getPath().compareTo(tUri2.getPath());
      }
    };
  }

  public String resetTachyonFileHierarchy() throws IOException {
    /**
     * Generate such local structure
     *  /testWildCards
     *  ├── foo
     *  |    ├── foobar1
     *  |    └── foobar2
     *  ├── bar
     *  |    └── foobar3
     *  └── foobar4
     */
    mTfs.delete(new TachyonURI("/testWildCards"), true);
    mTfs.mkdir(new TachyonURI("/testWildCards"));
    mTfs.mkdir(new TachyonURI("/testWildCards/foo"));
    mTfs.mkdir(new TachyonURI("/testWildCards/bar"));

    TachyonFSTestUtils.createByteFile(mTfs, "/testWildCards/foo/foobar1", WriteType.MUST_CACHE, 10);
    TachyonFSTestUtils.createByteFile(mTfs, "/testWildCards/foo/foobar2", WriteType.MUST_CACHE, 20);
    TachyonFSTestUtils.createByteFile(mTfs, "/testWildCards/bar/foobar3", WriteType.MUST_CACHE, 30);
    TachyonFSTestUtils.createByteFile(mTfs, "/testWildCards/foobar4", WriteType.MUST_CACHE, 40);

    return "/testWildCards";
  }

  public String resetLocalFileHierarchy() throws IOException {
    /**
     * Generate such local structure
     *  /testWildCards
     *  ├── foo
     *  |    ├── foobar1
     *  |    └── foobar2
     *  ├── bar
     *  |    └── foobar3
     *  └── foobar4
     */
    FileUtils.deleteDirectory(new File(mLocalTachyonCluster.getTachyonHome() + "/testWildCards"));
    new File(mLocalTachyonCluster.getTachyonHome() + "/testWildCards").mkdir();
    new File(mLocalTachyonCluster.getTachyonHome() + "/testWildCards/foo").mkdir();
    new File(mLocalTachyonCluster.getTachyonHome() + "/testWildCards/bar").mkdir();

    new File(mLocalTachyonCluster.getTachyonHome() + "/testWildCards/foo/foobar1").createNewFile();
    new File(mLocalTachyonCluster.getTachyonHome() + "/testWildCards/foo/foobar2").createNewFile();
    new File(mLocalTachyonCluster.getTachyonHome() + "/testWildCards/bar/foobar3").createNewFile();
    new File(mLocalTachyonCluster.getTachyonHome() + "/testWildCards/foobar4").createNewFile();

    return mLocalTachyonCluster.getTachyonHome() + "/testWildCards";
  }

  public List<String> getPaths(String path, FsType fsType) throws IOException {
    List<String> ret = null;
    if (fsType == FsType.TFS) {
      List<TachyonURI> tPaths = TFsShellUtils.getTachyonURIs(mTfs, new TachyonURI(path));
      ret = new ArrayList<String>(tPaths.size());
      for (TachyonURI tPath : tPaths) {
        ret.add(tPath.getPath());
      }
    } else if (fsType == FsType.LOCAL) {
      List<File> tPaths = TFsShellUtils.getFiles(path);
      ret = new ArrayList<String>(tPaths.size());
      for (File tPath : tPaths) {
        ret.add(tPath.getPath());
      }
    }
    Collections.sort(ret);
    return ret;
  }

  public String resetFsHierarchy(FsType fsType) throws IOException {
    if (fsType == FsType.TFS) {
      return resetTachyonFileHierarchy();
    } else if (fsType == FsType.LOCAL) {
      return resetLocalFileHierarchy();
    } else {
      return null;
    }
  }

  @Test
  public void getPathTest() throws IOException {
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
    Assert.assertEquals(TFsShellUtils.match("/a/b/c",  "/a/*"),    true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c",  "/a/*/"),   true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c",  "/a/*/c"),  true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c",  "/a/*/*"),  true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c",  "/a/*/*/"), true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c/", "/a/*/*/"), true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c/", "/a/*/*"),  true);

    Assert.assertEquals(TFsShellUtils.match("/foo/bar/foobar/", "/foo*/*"), true);
    Assert.assertEquals(TFsShellUtils.match("/foo/bar/foobar/", "/*/*/foobar"), true);

    Assert.assertEquals(TFsShellUtils.match("/a/b/c/", "/b/*"), false);
    Assert.assertEquals(TFsShellUtils.match("/", "/*/*"), false);

    Assert.assertEquals(TFsShellUtils.match("/a/b/c", "*"), true);
    Assert.assertEquals(TFsShellUtils.match("/", "/*"), true);
  }
}
