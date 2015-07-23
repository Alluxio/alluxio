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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;

/**
 * Unit tests on tachyon.command.Utils.
 *
 * Note that the test case for validatePath() is already covered in getFilePath. Hence only
 * getFilePathTest is specified.
 */
public class TFsShellUtilsTest {
  
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
  
  private void assertFilePath(File file, String path) {
    Assert.assertEquals(file.getAbsolutePath().compareTo(path), 0);
  }

  private void touchLocalFile(String path) throws IOException {
    File file = new File(path);
    if (file.exists() == false) {
      file.createNewFile();
    }
  }

  private boolean createLocalDir(String dirPath) throws IOException {
    File dir = new File(dirPath);
    return dir.mkdirs();
  }

  public static Comparator<File> createFilePathComparator() { 
    return  new Comparator<File>() {
      public int compare(File file1, File file2) {
        // ascending order
        return file1.getAbsoluteFile().compareTo(file2.getAbsoluteFile());
      }
    };
  }

  public void print(List<File> fileList) {
    for (File f : fileList) {
      System.out.println(f.getAbsolutePath());
    }
  }

  public String createRandomTempDir() {
    return FileUtils.getTempDirectoryPath() + new Double(Math.random()).hashCode();
  }
  
  @Test
  public void uriTest() {
    TachyonURI turi = new TachyonURI("/a/b/c");
    Assert.assertEquals(turi.getPathComponent(0), "/");
    Assert.assertEquals(turi.getPathComponent(1), "/a");
    Assert.assertEquals(turi.getPathComponent(2), "/a/b");
    Assert.assertEquals(turi.getPathComponent(3), "/a/b/c");
    Assert.assertEquals(turi.getPathComponent(4), null);
  }
 
  @Test
  public void getFilesTest() throws IOException {
    /**
     * Generate such local structure
     * rootDir
     *  ├── foo
     *  |    ├── foobar1
     *  |    └── foobar2
     *  └── bar
     *       └── foobar3
     */
    // generate a random path under rootDir
    String rootDir = createRandomTempDir();

    int i = 3; // we try three times;
    boolean created = false;
    while (i > 0) {
      try {
        // generate rootDir, if failure, we try three times
        if (createLocalDir(rootDir)) {
          created = true;
          break;
        }
      } catch (IOException e) { /* retry */ }
      rootDir = createRandomTempDir();
      i --;
    }

    String fooDir = rootDir + "/foo";
    String barDir = rootDir + "/bar";
    String foobar1 = fooDir + "/foobar1";
    String foobar2 = fooDir + "/foobar2";
    String foobar3 = barDir + "/foobar3";

    if (created == false) {
      return;
    }

    try {
      createLocalDir(fooDir);
      createLocalDir(barDir);
      touchLocalFile(foobar1);
      touchLocalFile(foobar2);
      touchLocalFile(foobar3);
    } catch (IOException e) {
      return; // ignore the latter tests if the environment setting fails.
    }

    List<File> fl1 = TFsShellUtils.getFiles(rootDir + "/foo");
    print(fl1);
    Collections.sort(fl1, createFilePathComparator());
    Assert.assertEquals(fl1.size(), 1);
    assertFilePath(fl1.get(0), rootDir + "/foo");

    // Trailing slash
    List<File> fl2 = TFsShellUtils.getFiles(rootDir + "/foo/");
    Collections.sort(fl2, createFilePathComparator());
    Assert.assertEquals(fl2.size(), 1);
    assertFilePath(fl2.get(0), rootDir + "/foo");

    // Wildcard
    List<File> fl3 = TFsShellUtils.getFiles(rootDir + "/foo/*");
    Collections.sort(fl3, createFilePathComparator());
    Assert.assertEquals(fl3.size(), 1);
    assertFilePath(fl3.get(0), rootDir + "/foo/foobar1");
    assertFilePath(fl3.get(1), rootDir + "/foo/foobar2");

    // Trailing slash + wildcard
    List<File> fl4 = TFsShellUtils.getFiles("/tmp/foo/*/");
    Collections.sort(fl4, createFilePathComparator());
    Assert.assertEquals(fl4.size(), 1);
    assertFilePath(fl4.get(0), rootDir + "/foo/foobar1");
    assertFilePath(fl4.get(1), rootDir + "/foo/foobar2");

    // Multiple wildcards
    List<File> fl5 = TFsShellUtils.getFiles("/tmp/*/foo*");
    Collections.sort(fl5, createFilePathComparator());
    Assert.assertEquals(fl5.size(), 3);
    assertFilePath(fl5.get(0), rootDir + "/foo/foobar1");
    assertFilePath(fl5.get(1), rootDir + "/bar/foobar2");
    assertFilePath(fl5.get(2), rootDir + "/bar/foobar3");

    FileUtils.deleteDirectory(new File(rootDir));
  }
  
  @Test
  public void matchTest() {
    Assert.assertEquals(TFsShellUtils.match("/a/b/c", "/a/*"), true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c", "/a/*/"), true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c", "/a/*/c"), true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c", "/a/*/*"), true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c", "/a/*/*/"), true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c/", "/a/*/*/"), true);
    Assert.assertEquals(TFsShellUtils.match("/a/b/c/", "/a/*/*"), true);
    
    Assert.assertEquals(TFsShellUtils.match("/foo/bar/foobar/", "/foo*/*"), true);
    Assert.assertEquals(TFsShellUtils.match("/foo/bar/foobar/", "/*/*/foobar"), true);
    
    Assert.assertEquals(TFsShellUtils.match("/a/b/c/", "/b/*"), false);
    Assert.assertEquals(TFsShellUtils.match("/", "/*/*"), false);
  }
}
