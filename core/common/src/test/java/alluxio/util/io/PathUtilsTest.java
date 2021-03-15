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

package alluxio.util.io;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

/**
 * Tests for the {@link PathUtils} class.
 */
public final class PathUtilsTest {

  /**
   * The expected exception thrown during a test.
   */
  @Rule
  public final ExpectedException mException = ExpectedException.none();

  /**
   * Tests the {@link PathUtils#cleanPath(String)} method.
   */
  @Test
  public void cleanPathNoException() throws InvalidPathException {
    // test clean path
    assertEquals("/foo/bar", PathUtils.cleanPath("/foo/bar"));

    // test trailing slash
    assertEquals("/foo/bar", PathUtils.cleanPath("/foo/bar/"));

    // test redundant slashes
    assertEquals("/foo/bar", PathUtils.cleanPath("/foo//bar"));
    assertEquals("/foo/bar", PathUtils.cleanPath("/foo//bar//"));
    assertEquals("/foo/bar", PathUtils.cleanPath("/foo///////bar//////"));

    // test dots gets resolved
    assertEquals("/foo/bar", PathUtils.cleanPath("/foo/./bar"));
    assertEquals("/foo/bar", PathUtils.cleanPath("/foo/././bar"));
    assertEquals("/foo", PathUtils.cleanPath("/foo/bar/.."));
    assertEquals("/bar", PathUtils.cleanPath("/foo/../bar"));
    assertEquals("/", PathUtils.cleanPath("/foo/bar/../.."));

    // the following seems strange
    // TODO(jiri): Instead of returning null, throw InvalidPathException.
    assertNull(PathUtils.cleanPath("/foo/bar/../../.."));
  }

  /**
   * Tests the {@link PathUtils#cleanPath(String)} method to thrown an exception in case an invalid
   * path is provided.
   */
  @Test
  public void cleanPathException() throws InvalidPathException {
    mException.expect(InvalidPathException.class);
    PathUtils.cleanPath("");
  }

  /**
   * Tests the {@link PathUtils#concatPath(Object, Object...)} method.
   */
  @Test
  public void concatPath() {
    assertEquals("/", PathUtils.concatPath("/"));
    assertEquals("/", PathUtils.concatPath("/", ""));
    assertEquals("/bar", PathUtils.concatPath("/", "bar"));

    assertEquals("foo", PathUtils.concatPath("foo"));
    assertEquals("/foo", PathUtils.concatPath("/foo"));
    assertEquals("/foo", PathUtils.concatPath("/foo", ""));

    // Join base without trailing "/"
    assertEquals("/foo/bar", PathUtils.concatPath("/foo", "bar"));
    assertEquals("/foo/bar", PathUtils.concatPath("/foo", "bar/"));
    assertEquals("/foo/bar", PathUtils.concatPath("/foo", "/bar"));
    assertEquals("/foo/bar", PathUtils.concatPath("/foo", "/bar/"));

    // Join base with trailing "/"
    assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar"));
    assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar/"));
    assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "/bar"));
    assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "/bar/"));

    // Redundant separator must be trimmed.
    assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar//"));

    // Multiple components to join.
    assertEquals("/foo/bar/a/b/c", PathUtils.concatPath("/foo", "bar", "a", "b", "c"));
    assertEquals("/foo/bar/b/c", PathUtils.concatPath("/foo", "bar", "", "b", "c"));

    // Non-string
    assertEquals("/foo/bar/1", PathUtils.concatPath("/foo", "bar", 1));
    assertEquals("/foo/bar/2", PathUtils.concatPath("/foo", "bar", 2L));

    // Header
    assertEquals(Constants.HEADER + "host:port/foo/bar",
        PathUtils.concatPath(Constants.HEADER + "host:port", "/foo", "bar"));
  }

  /**
   * Tests the {@link PathUtils#findLowestCommonAncestor(Collection)} method.
   */
  @Test
  public void findLowestCommonAncestor() {
    assertNull(PathUtils.findLowestCommonAncestor(null));
    assertNull(PathUtils.findLowestCommonAncestor(Collections.EMPTY_LIST));

    ArrayList<AlluxioURI> paths = new ArrayList<>();

    paths.add(new AlluxioURI("/"));
    assertEquals("/", PathUtils.findLowestCommonAncestor(paths).getPath());

    paths.clear();
    paths.add(new AlluxioURI("/a"));
    assertEquals("/a", PathUtils.findLowestCommonAncestor(paths).getPath());

    paths.add(new AlluxioURI("/a/b"));
    assertEquals("/a", PathUtils.findLowestCommonAncestor(paths).getPath());

    paths.clear();
    paths.add(new AlluxioURI("/a/c"));
    paths.add(new AlluxioURI("/a/d/"));
    assertEquals("/a", PathUtils.findLowestCommonAncestor(paths).getPath());

    paths.add(new AlluxioURI("/b/a/"));
    assertEquals("/", PathUtils.findLowestCommonAncestor(paths).getPath());

    paths.clear();
    paths.add(new AlluxioURI("/a/b/c"));
    paths.add(new AlluxioURI("/a/b/d"));
    paths.add(new AlluxioURI("/a/b/e"));
    assertEquals("/a/b", PathUtils.findLowestCommonAncestor(paths).getPath());

    paths.clear();
    String prefix = "/a/b/c";
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        for (int k = 0; k < 10; k++) {
          paths.add(new AlluxioURI(String.format("%s/%d/%d/%d", prefix, i, j, k)));
        }
      }
    }
    assertEquals(prefix, PathUtils.findLowestCommonAncestor(paths).getPath());
  }

  /**
   * Tests the {@link PathUtils#getParent(String)} method.
   */
  @Test
  public void getParent() throws InvalidPathException {
    // get a parent that is non-root
    assertEquals("/foo", PathUtils.getParent("/foo/bar"));
    assertEquals("/foo", PathUtils.getParent("/foo/bar/"));
    assertEquals("/foo", PathUtils.getParent("/foo/./bar/"));
    assertEquals("/foo", PathUtils.getParent("/foo/././bar/"));

    // get a parent that is root
    assertEquals("/", PathUtils.getParent("/foo"));
    assertEquals("/", PathUtils.getParent("/foo/bar/../"));
    assertEquals("/", PathUtils.getParent("/foo/../bar/"));

    // get parent of root
    assertEquals("/", PathUtils.getParent("/"));
    assertEquals("/", PathUtils.getParent("/foo/bar/../../"));
    assertEquals("/", PathUtils.getParent("/foo/../bar/../"));
  }

  @Test
  public void concatUfsPath() {
    assertEquals("s3://", PathUtils.concatUfsPath("s3://", ""));
    assertEquals("s3://bar", PathUtils.concatUfsPath("s3://", "bar"));

    assertEquals("hdfs://localhost:9010",
        PathUtils.concatUfsPath("hdfs://localhost:9010/", ""));
    assertEquals("hdfs://localhost:9010/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/", "bar"));

    assertEquals("s3://foo", PathUtils.concatUfsPath("s3://foo", ""));
    assertEquals("hdfs://localhost:9010/foo",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo", ""));

    // Join base without trailing "/"
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo", "bar"));
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo", "bar/"));
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo", "/bar"));
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo", "/bar/"));

    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo", "bar"));
    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo", "bar/"));
    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo", "/bar"));
    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo", "/bar/"));

    // Join base with trailing "/"
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo/", "bar"));
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo/", "bar/"));
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo/", "/bar"));
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo/", "/bar/"));

    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo/", "bar"));
    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo/", "bar/"));
    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo/", "/bar"));
    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo/", "/bar/"));

    // Redundant separator must be trimmed.
    assertEquals("s3://foo/bar", PathUtils.concatUfsPath("s3://foo/", "bar//"));
    assertEquals("hdfs://localhost:9010/foo/bar",
        PathUtils.concatUfsPath("hdfs://localhost:9010/foo/", "bar//"));
  }

  @Test
  public void getPersistentTmpPath() {
    // Get temporary path
    Pattern pattern = Pattern.compile(
        "\\.alluxio_ufs_persistence\\/test\\.parquet\\.alluxio\\.\\d+\\.\\S+\\.tmp");
    String tempPersistencePath = PathUtils.getPersistentTmpPath("s3://test/test.parquet");
    assertEquals(pattern.matcher(tempPersistencePath).matches(), true);
    pattern = Pattern.compile(
        "\\.alluxio_ufs_persistence\\/test\\.parquet\\.alluxio\\.\\d+\\.\\S+\\.tmp");
    tempPersistencePath = PathUtils.getPersistentTmpPath("hdfs://localhost:9010/test/test.parquet");
    assertEquals(pattern.matcher(tempPersistencePath).matches(), true);

    // Get temporary path with root path
    pattern = Pattern.compile(
        "\\.alluxio_ufs_persistence\\/test\\.parquet\\.alluxio\\.\\d+\\.\\S+\\.tmp");
    tempPersistencePath = PathUtils.getPersistentTmpPath("s3://test.parquet");
    assertEquals(pattern.matcher(tempPersistencePath).matches(), true);
    pattern = Pattern.compile(
        "\\.alluxio_ufs_persistence\\/test\\.parquet\\.alluxio\\.\\d+\\.\\S+\\.tmp");
    tempPersistencePath = PathUtils.getPersistentTmpPath("hdfs://localhost:9010/test.parquet");
    assertEquals(pattern.matcher(tempPersistencePath).matches(), true);
  }

  /**
   * Tests the {@link PathUtils#getPathComponents(String)} method.
   */
  @Test
  public void getPathComponentsNoException() throws InvalidPathException {
    assertArrayEquals(new String[] {""}, PathUtils.getPathComponents("/"));
    assertArrayEquals(new String[] {"", "bar"}, PathUtils.getPathComponents("/bar"));
    assertArrayEquals(new String[] {"", "foo", "bar"},
        PathUtils.getPathComponents("/foo/bar"));
    assertArrayEquals(new String[] {"", "foo", "bar"},
        PathUtils.getPathComponents("/foo/bar/"));
    assertArrayEquals(new String[] {"", "bar"},
        PathUtils.getPathComponents("/foo/../bar"));
    assertArrayEquals(new String[] {"", "foo", "bar", "a", "b", "c"},
        PathUtils.getPathComponents("/foo//bar/a/b/c"));
  }

  /**
   * Tests the {@link PathUtils#getPathComponents(String)} method to thrown an exception in case the
   * path is invalid.
   */
  @Test
  public void getPathComponentsException() throws InvalidPathException {
    mException.expect(InvalidPathException.class);
    PathUtils.getPathComponents("");
  }

  /**
   * Tests the {@link PathUtils#subtractPaths(String, String)} method.
   */
  @Test
  public void subtractPaths() throws InvalidPathException {
    assertEquals("b/c", PathUtils.subtractPaths("/a/b/c", "/a"));
    assertEquals("b/c", PathUtils.subtractPaths("/a/b/c", "/a/"));
    assertEquals("b/c", PathUtils.subtractPaths("/a/b/c", "/a/"));
    assertEquals("c", PathUtils.subtractPaths("/a/b/c", "/a/b"));
    assertEquals("a/b/c", PathUtils.subtractPaths("/a/b/c", "/"));
    assertEquals("", PathUtils.subtractPaths("/", "/"));
    assertEquals("", PathUtils.subtractPaths("/a/b/", "/a/b"));
    assertEquals("", PathUtils.subtractPaths("/a/b", "/a/b"));
  }

  /**
   * Tests {@link PathUtils#subtractPaths(String, String)} throws the right exception if an input
   * path is invalid or the second argument isn't a prefix of the first.
   */
  @Test
  public void subtractPathsException() throws InvalidPathException {
    try {
      PathUtils.subtractPaths("", "/");
      fail("\"\" should throw an InvalidPathException");
    } catch (InvalidPathException e) {
      assertEquals(ExceptionMessage.PATH_INVALID.getMessage(""), e.getMessage());
    }
    try {
      PathUtils.subtractPaths("/", "noslash");
      fail("noslash should be an invalid path");
    } catch (InvalidPathException e) {
      assertEquals(ExceptionMessage.PATH_INVALID.getMessage("noslash"), e.getMessage());
    }
    try {
      PathUtils.subtractPaths("/a", "/not/a/prefix");
      fail("subtractPaths should complain about the prefix not being a prefix");
    } catch (RuntimeException e) {
      String expectedMessage = "Cannot subtract /not/a/prefix from /a because it is not a prefix";
      assertEquals(expectedMessage, e.getMessage());
    }
  }

  /**
   * Tests the {@link PathUtils#hasPrefix(String, String)} method.
   */
  @Test
  public void hasPrefix() throws InvalidPathException {
    assertTrue(PathUtils.hasPrefix("/", "/"));
    assertTrue(PathUtils.hasPrefix("/a", "/a"));
    assertTrue(PathUtils.hasPrefix("/a", "/a/"));
    assertTrue(PathUtils.hasPrefix("/a/b/c", "/a"));
    assertTrue(PathUtils.hasPrefix("/a/b/c", "/a/b"));
    assertTrue(PathUtils.hasPrefix("/a/b/c", "/a/b/c"));
    assertFalse(PathUtils.hasPrefix("/", "/a"));
    assertFalse(PathUtils.hasPrefix("/", "/a/b/c"));
    assertFalse(PathUtils.hasPrefix("/a", "/a/b/c"));
    assertFalse(PathUtils.hasPrefix("/a/b", "/a/b/c"));
    assertFalse(PathUtils.hasPrefix("/a/b/c", "/aa"));
    assertFalse(PathUtils.hasPrefix("/a/b/c", "/a/bb"));
    assertFalse(PathUtils.hasPrefix("/a/b/c", "/a/b/cc"));
    assertFalse(PathUtils.hasPrefix("/aa/b/c", "/a"));
    assertFalse(PathUtils.hasPrefix("/a/bb/c", "/a/b"));
    assertFalse(PathUtils.hasPrefix("/a/b/cc", "/a/b/c"));
  }

  /**
   * Tests the {@link PathUtils#isRoot(String)} method.
   */
  @Test
  public void isRoot() throws InvalidPathException {
    // check a path that is non-root
    assertFalse(PathUtils.isRoot("/foo/bar"));
    assertFalse(PathUtils.isRoot("/foo/bar/"));
    assertFalse(PathUtils.isRoot("/foo/./bar/"));
    assertFalse(PathUtils.isRoot("/foo/././bar/"));
    assertFalse(PathUtils.isRoot("/foo/../bar"));
    assertFalse(PathUtils.isRoot("/foo/../bar/"));

    // check a path that is root
    assertTrue(PathUtils.isRoot("/"));
    assertTrue(PathUtils.isRoot("/"));
    assertTrue(PathUtils.isRoot("/."));
    assertTrue(PathUtils.isRoot("/./"));
    assertTrue(PathUtils.isRoot("/foo/.."));
    assertTrue(PathUtils.isRoot("/foo/../"));
  }

  /**
   * Tests the {@link PathUtils#temporaryFileName(long, String)} method.
   */
  @Test
  public void temporaryFileName() {
    assertEquals(PathUtils.temporaryFileName(1, "/"),
        PathUtils.temporaryFileName(1, "/"));
    assertNotEquals(PathUtils.temporaryFileName(1, "/"),
        PathUtils.temporaryFileName(2, "/"));
    assertNotEquals(PathUtils.temporaryFileName(1, "/"),
        PathUtils.temporaryFileName(1, "/a"));
  }

  @Test
  public void getPermanentFileName() {
    assertEquals("/", PathUtils.getPermanentFileName(PathUtils.temporaryFileName(1, "/")));
    assertEquals("/",
        PathUtils.getPermanentFileName(PathUtils.temporaryFileName(0xFFFFFFFFFFFFFFFFL, "/")));
    assertEquals("/foo.alluxio.0x0123456789ABCDEF.tmp", PathUtils
        .getPermanentFileName(PathUtils.temporaryFileName(14324,
            "/foo.alluxio.0x0123456789ABCDEF.tmp")));
  }

  /**
   * Test the {@link PathUtils#isTemporaryFileName(String)} method.
   */
  @Test
  public void isTemporaryFileName() {
    assertTrue(PathUtils.isTemporaryFileName(PathUtils.temporaryFileName(0, "/")));
    assertTrue(
        PathUtils.isTemporaryFileName(PathUtils.temporaryFileName(0xFFFFFFFFFFFFFFFFL, "/")));
    assertTrue(PathUtils.isTemporaryFileName("foo.alluxio.0x0123456789ABCDEF.tmp"));
    assertFalse(PathUtils.isTemporaryFileName("foo.alluxio.0x      0123456789.tmp"));
    assertFalse(PathUtils.isTemporaryFileName("foo.alluxio.0x0123456789ABCDEFG.tmp"));
    assertFalse(PathUtils.isTemporaryFileName("foo.alluxio.0x0123456789ABCDE.tmp"));
    assertFalse(PathUtils.isTemporaryFileName("foo.0x0123456789ABCDEFG.tmp"));
    assertFalse(PathUtils.isTemporaryFileName("alluxio.0x0123456789ABCDEFG"));
  }

  /**
   * Tests the {@link PathUtils#uniqPath()} method.
   */
  @Test
  public void uniqPath() {
    assertNotEquals(PathUtils.uniqPath(), PathUtils.uniqPath());
  }

  /**
   * Tests the {@link PathUtils#validatePath(String)} method.
   *
   * @throws InvalidPathException thrown if the path is invalid
   */
  @Test
  public void validatePath() throws InvalidPathException {
    // check valid paths
    PathUtils.validatePath("/foo/bar");
    PathUtils.validatePath("/foo/bar/");
    PathUtils.validatePath("/foo/./bar/");
    PathUtils.validatePath("/foo/././bar/");
    PathUtils.validatePath("/foo/../bar");
    PathUtils.validatePath("/foo/../bar/");
    // check invalid paths
    ArrayList<String> invalidPaths = new ArrayList<>();
    invalidPaths.add(null);
    invalidPaths.add("");
    invalidPaths.add("not a path");
    for (String invalidPath : invalidPaths) {
      try {
        PathUtils.validatePath(invalidPath);
        fail("validatePath(" + invalidPath + ") did not fail");
      } catch (InvalidPathException e) {
        // this is expected
      }
    }
  }

  /**
   * Tests the {@link PathUtils#normalizePath(String, String)} method.
   */
  @Test
  public void normalizePath() throws Exception {
    assertEquals("/", PathUtils.normalizePath("", "/"));
    assertEquals("/", PathUtils.normalizePath("/", "/"));
    assertEquals("/foo/bar/", PathUtils.normalizePath("/foo/bar", "/"));
    assertEquals("/foo/bar/", PathUtils.normalizePath("/foo/bar/", "/"));
    assertEquals("/foo/bar//", PathUtils.normalizePath("/foo/bar//", "/"));
    assertEquals("/foo/bar%", PathUtils.normalizePath("/foo/bar", "%"));
  }
}
