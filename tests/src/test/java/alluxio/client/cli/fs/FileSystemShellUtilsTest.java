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

package alluxio.client.cli.fs;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.cli.Command;
import alluxio.cli.fs.FileSystemShell;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.reflections.Reflections;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Unit tests on {@link FileSystemShellUtils}.
 *
 * Note that the test case for {@link FileSystemShellUtils#validatePath(String)} is already covered
 * in {@link FileSystemShellUtils#getFilePath(String)}. Hence only getFilePathTest is specified.
 */
public final class FileSystemShellUtilsTest {
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
        new String[] {Constants.HEADER + "localhost:19998/dir", "/dir", "dir"};
    String expected = "/dir";
    for (String path : paths) {
      String result = FileSystemShellUtils.getFilePath(path, ConfigurationTestUtils.defaults());
      assertEquals(expected, result);
    }
  }

  /** Type of file system. */
  public enum FsType {
    TFS, LOCAL
  }

  // TODO(binfan): rename resetFileHierarchy to resetFileSystemHierarchy
  // TODO(binfan): use option idiom for FileSystem fs, WriteType writeType
  // TODO(binfan): move those static methods to a util class
  public String resetFileHierarchy() throws IOException, AlluxioException {
    return resetFileHierarchy(mFileSystem);
  }

  /**
   * Resets the file hierarchy.
   *
   * @param fs the file system
   * @return the test directory
   */
  public static String resetFileHierarchy(FileSystem fs) throws IOException, AlluxioException {
    return resetFileHierarchy(fs, WritePType.MUST_CACHE);
  }

  /**
   * Resets the file hierarchy.
   *
   * @param fs the file system
   * @param writeType write types for creating a file in Alluxio
   * @return the test directory
   */
  public static String resetFileHierarchy(FileSystem fs, WritePType writeType)
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
      fs.delete(new AlluxioURI(TEST_DIR), DeletePOptions.newBuilder().setRecursive(true).build());
    }
    CreateDirectoryPOptions dirOptions = CreateDirectoryPOptions.getDefaultInstance().toBuilder()
        .setWriteType(writeType).build();
    fs.createDirectory(new AlluxioURI(TEST_DIR), dirOptions);
    fs.createDirectory(new AlluxioURI(TEST_DIR + "/foo"), dirOptions);
    fs.createDirectory(new AlluxioURI(TEST_DIR + "/bar"), dirOptions);

    FileSystemTestUtils.createByteFile(fs, TEST_DIR + "/foo/foobar1", writeType, 10);
    FileSystemTestUtils.createByteFile(fs, TEST_DIR + "/foo/foobar2", writeType, 20);
    FileSystemTestUtils.createByteFile(fs, TEST_DIR + "/bar/foobar3", writeType, 30);
    FileSystemTestUtils.createByteFile(fs, TEST_DIR + "/foobar4", writeType, 40);
    return TEST_DIR;
  }

  /**
   * Resets the local file hierarchy.
   *
   * @return the local test directory
   */
  public String resetLocalFileHierarchy() throws IOException {
    return resetLocalFileHierarchy(mLocalAlluxioClusterResource.get());
  }

  /**
   * Resets the local file hierarchy.
   *
   * @param localAlluxioCluster local Alluxio cluster for tests
   * @return the local test directory
   */
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

  /**
   * Gets all the file paths that match the inputPath depending on fsType.
   *
   * @param path the input path
   * @param fsType the type of file system
   * @return a list of files that matches inputPath
   */
  public List<String> getPaths(String path, FsType fsType) throws IOException {
    List<String> ret = null;
    if (fsType == FsType.TFS) {
      List<AlluxioURI> tPaths =
          FileSystemShellUtils.getAlluxioURIs(mFileSystem, new AlluxioURI(path));
      ret = new ArrayList<>(tPaths.size());
      for (AlluxioURI tPath : tPaths) {
        ret.add(tPath.getPath());
      }
    } else if (fsType == FsType.LOCAL) {
      List<File> tPaths = FileSystemShellUtils.getFiles(path);
      ret = new ArrayList<>(tPaths.size());
      for (File tPath : tPaths) {
        ret.add(tPath.getPath());
      }
    }
    Collections.sort(ret);
    return ret;
  }

  /**
   * Resets the file hierarchy depending on the type of file system.
   *
   * @param fsType the type of file system
   * @return the test directory, null if the fsType is invalid
   */
  @Nullable
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
  public void getPath() throws IOException, AlluxioException {
    for (FsType fsType : FsType.values()) {
      String rootDir = resetFsHierarchy(fsType);

      List<String> tl1 = getPaths(rootDir + "/foo", fsType);
      assertEquals(tl1.size(), 1);
      assertEquals(tl1.get(0), rootDir + "/foo");

      // Trailing slash
      List<String> tl2 = getPaths(rootDir + "/foo/", fsType);
      assertEquals(tl2.size(), 1);
      assertEquals(tl2.get(0), rootDir + "/foo");

      // Wildcard
      List<String> tl3 = getPaths(rootDir + "/foo/*", fsType);
      assertEquals(tl3.size(), 2);
      assertEquals(tl3.get(0), rootDir + "/foo/foobar1");
      assertEquals(tl3.get(1), rootDir + "/foo/foobar2");

      // Trailing slash + wildcard
      List<String> tl4 = getPaths(rootDir + "/foo/*/", fsType);
      assertEquals(tl4.size(), 2);
      assertEquals(tl4.get(0), rootDir + "/foo/foobar1");
      assertEquals(tl4.get(1), rootDir + "/foo/foobar2");

      // Multiple wildcards
      List<String> tl5 = getPaths(rootDir + "/*/foo*", fsType);
      assertEquals(tl5.size(), 3);
      assertEquals(tl5.get(0), rootDir + "/bar/foobar3");
      assertEquals(tl5.get(1), rootDir + "/foo/foobar1");
      assertEquals(tl5.get(2), rootDir + "/foo/foobar2");
    }
  }

  @Test
  public void match() {
    assertEquals(FileSystemShellUtils.match("/a/b/c", "/a/*"), true);
    assertEquals(FileSystemShellUtils.match("/a/b/c", "/a/*/"), true);
    assertEquals(FileSystemShellUtils.match("/a/b/c", "/a/*/c"), true);
    assertEquals(FileSystemShellUtils.match("/a/b/c", "/a/*/*"), true);
    assertEquals(FileSystemShellUtils.match("/a/b/c", "/a/*/*/"), true);
    assertEquals(FileSystemShellUtils.match("/a/b/c/", "/a/*/*/"), true);
    assertEquals(FileSystemShellUtils.match("/a/b/c/", "/a/*/*"), true);

    assertEquals(FileSystemShellUtils.match("/foo/bar/foobar/", "/foo*/*"), true);
    assertEquals(FileSystemShellUtils.match("/foo/bar/foobar/", "/*/*/foobar"), true);

    assertEquals(FileSystemShellUtils.match("/a/b/c/", "/b/*"), false);
    assertEquals(FileSystemShellUtils.match("/", "/*/*"), false);

    assertEquals(FileSystemShellUtils.match("/a/b/c", "*"), true);
    assertEquals(FileSystemShellUtils.match("/", "/*"), true);
  }

  @Test
  public void getIntArgTest() throws Exception {
    Option opt = Option.builder("t")
        .longOpt("test")
        .numberOfArgs(1)
        .required(false)
        .build();

    CommandLine cmdLine = getCmdLine(opt, "--test", "1");
    assertEquals("Should get long form", 1, FileSystemShellUtils.getIntArg(cmdLine, opt, 0));

    cmdLine = getCmdLine(opt, "-t", "5");
    assertEquals("Should get short form", 5, FileSystemShellUtils.getIntArg(cmdLine, opt, 0));

    cmdLine = getCmdLine(opt);
    assertEquals("Should not get arg", 0, FileSystemShellUtils.getIntArg(cmdLine, opt, 0));
  }

  CommandLine getCmdLine(Option opt, String... args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    return parser.parse(new Options().addOption(opt), args);
  }

  @Test
  public void loadCommands() {
    Map<String, Command> map =
        FileSystemShellUtils.loadCommands(FileSystemContext.create(ServerConfiguration.global()));

    String pkgName = Command.class.getPackage().getName();
    Reflections reflections = new Reflections(pkgName);
    Set<Class<? extends Command>> cmdSet = reflections.getSubTypesOf(Command.class);
    for (Map.Entry<String, Command> entry : map.entrySet()) {
      assertEquals(entry.getValue().getCommandName(), entry.getKey());
      assertEquals(cmdSet.contains(entry.getValue().getClass()), true);
    }

    int expectSize = 0;
    for (Class<? extends Command> cls : cmdSet) {
      if (cls.getPackage().getName()
          .equals(FileSystemShell.class.getPackage().getName() + ".command")
          && !Modifier.isAbstract(cls.getModifiers())) {
        expectSize++;
      }
    }
    assertEquals(expectSize, map.size());
  }
}

