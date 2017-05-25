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

import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.LinkedList;

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
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/foo/bar"));

    // test trailing slash
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/foo/bar/"));

    // test redundant slashes
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/foo//bar"));
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/foo//bar//"));
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/foo///////bar//////"));

    // test dots gets resolved
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/foo/./bar"));
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/foo/././bar"));
    Assert.assertEquals("/foo", PathUtils.cleanPath("/foo/bar/.."));
    Assert.assertEquals("/bar", PathUtils.cleanPath("/foo/../bar"));
    Assert.assertEquals("/", PathUtils.cleanPath("/foo/bar/../.."));

    // the following seems strange
    // TODO(jiri): Instead of returning null, throw InvalidPathException.
    Assert.assertNull(PathUtils.cleanPath("/foo/bar/../../.."));
  }

  /**
   * Tests the {@link PathUtils#cleanPath(String)} method to thrown an exception in case an invalid
   * path is provided.
   */
  @Test
  public void cleanPathException() throws InvalidPathException {
    mException.expect(InvalidPathException.class);
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/\\   foo / bar"));
  }

  /**
   * Tests the {@link PathUtils#concatPath(Object, Object...)} method.
   */
  @Test
  public void concatPath() {
    Assert.assertEquals("/", PathUtils.concatPath("/"));
    Assert.assertEquals("/", PathUtils.concatPath("/", ""));
    Assert.assertEquals("/bar", PathUtils.concatPath("/", "bar"));

    Assert.assertEquals("foo", PathUtils.concatPath("foo"));
    Assert.assertEquals("/foo", PathUtils.concatPath("/foo"));
    Assert.assertEquals("/foo", PathUtils.concatPath("/foo", ""));

    // Join base without trailing "/"
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo", "bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo", "bar/"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo", "/bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo", "/bar/"));

    // Join base with trailing "/"
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar/"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "/bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "/bar/"));

    // Whitespace must be trimmed.
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo ", "bar  "));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo ", "  bar"));
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo ", "  bar  "));

    // Redundant separator must be trimmed.
    Assert.assertEquals("/foo/bar", PathUtils.concatPath("/foo/", "bar//"));

    // Multiple components to join.
    Assert.assertEquals("/foo/bar/a/b/c", PathUtils.concatPath("/foo", "bar", "a", "b", "c"));
    Assert.assertEquals("/foo/bar/b/c", PathUtils.concatPath("/foo", "bar", "", "b", "c"));

    // Non-string
    Assert.assertEquals("/foo/bar/1", PathUtils.concatPath("/foo", "bar", 1));
    Assert.assertEquals("/foo/bar/2", PathUtils.concatPath("/foo", "bar", 2L));

    // Header
    Assert.assertEquals(Constants.HEADER + "host:port/foo/bar",
        PathUtils.concatPath(Constants.HEADER + "host:port", "/foo", "bar"));
  }

  /**
   * Tests the {@link PathUtils#getParent(String)} method.
   */
  @Test
  public void getParent() throws InvalidPathException {
    // get a parent that is non-root
    Assert.assertEquals("/foo", PathUtils.getParent("/foo/bar"));
    Assert.assertEquals("/foo", PathUtils.getParent("/foo/bar/"));
    Assert.assertEquals("/foo", PathUtils.getParent("/foo/./bar/"));
    Assert.assertEquals("/foo", PathUtils.getParent("/foo/././bar/"));

    // get a parent that is root
    Assert.assertEquals("/", PathUtils.getParent("/foo"));
    Assert.assertEquals("/", PathUtils.getParent("/foo/bar/../"));
    Assert.assertEquals("/", PathUtils.getParent("/foo/../bar/"));

    // get parent of root
    Assert.assertEquals("/", PathUtils.getParent("/"));
    Assert.assertEquals("/", PathUtils.getParent("/foo/bar/../../"));
    Assert.assertEquals("/", PathUtils.getParent("/foo/../bar/../"));
  }

  /**
   * Tests the {@link PathUtils#getPathComponents(String)} method.
   */
  @Test
  public void getPathComponentsNoException() throws InvalidPathException {
    Assert.assertArrayEquals(new String[] {""}, PathUtils.getPathComponents("/"));
    Assert.assertArrayEquals(new String[] {"", "bar"}, PathUtils.getPathComponents("/bar"));
    Assert.assertArrayEquals(new String[] {"", "foo", "bar"},
        PathUtils.getPathComponents("/foo/bar"));
    Assert.assertArrayEquals(new String[] {"", "foo", "bar"},
        PathUtils.getPathComponents("/foo/bar/"));
    Assert.assertArrayEquals(new String[] {"", "bar"},
        PathUtils.getPathComponents("/foo/../bar"));
    Assert.assertArrayEquals(new String[] {"", "foo", "bar", "a", "b", "c"},
        PathUtils.getPathComponents("/foo//bar/a/b/c"));
  }

  /**
   * Tests the {@link PathUtils#getPathComponents(String)} method to thrown an exception in case the
   * path is invalid.
   */
  @Test
  public void getPathComponentsException() throws InvalidPathException {
    mException.expect(InvalidPathException.class);
    PathUtils.getPathComponents("/\\   foo / bar");
  }

  /**
   * Tests the {@link PathUtils#subtractPaths(String, String)} method.
   */
  @Test
  public void subtractPaths() throws InvalidPathException {
    Assert.assertEquals("b/c", PathUtils.subtractPaths("/a/b/c", "/a"));
    Assert.assertEquals("b/c", PathUtils.subtractPaths("/a/b/c", "/a/"));
    Assert.assertEquals("b/c", PathUtils.subtractPaths("/a/b/c", "/a/"));
    Assert.assertEquals("c", PathUtils.subtractPaths("/a/b/c", "/a/b"));
    Assert.assertEquals("a/b/c", PathUtils.subtractPaths("/a/b/c", "/"));
    Assert.assertEquals("", PathUtils.subtractPaths("/", "/"));
    Assert.assertEquals("", PathUtils.subtractPaths("/a/b/", "/a/b"));
    Assert.assertEquals("", PathUtils.subtractPaths("/a/b", "/a/b"));
  }

  /**
   * Tests {@link PathUtils#subtractPaths(String, String)} throws the right exception if an input
   * path is invalid or the second argument isn't a prefix of the first.
   */
  @Test
  public void subtractPathsException() throws InvalidPathException {
    try {
      PathUtils.subtractPaths("", "/");
      Assert.fail("\"\" should throw an InvalidPathException");
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.PATH_INVALID.getMessage(""), e.getMessage());
    }
    try {
      PathUtils.subtractPaths("/", "noslash");
      Assert.fail("noslash should be an invalid path");
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.PATH_INVALID.getMessage("noslash"), e.getMessage());
    }
    try {
      PathUtils.subtractPaths("/a", "/not/a/prefix");
      Assert.fail("subtractPaths should complain about the prefix not being a prefix");
    } catch (RuntimeException e) {
      String expectedMessage = "Cannot subtract /not/a/prefix from /a because it is not a prefix";
      Assert.assertEquals(expectedMessage, e.getMessage());
    }
  }

  /**
   * Tests the {@link PathUtils#hasPrefix(String, String)} method.
   */
  @Test
  public void hasPrefix() throws InvalidPathException {
    Assert.assertTrue(PathUtils.hasPrefix("/", "/"));
    Assert.assertTrue(PathUtils.hasPrefix("/a", "/a"));
    Assert.assertTrue(PathUtils.hasPrefix("/a", "/a/"));
    Assert.assertTrue(PathUtils.hasPrefix("/a/b/c", "/a"));
    Assert.assertTrue(PathUtils.hasPrefix("/a/b/c", "/a/b"));
    Assert.assertTrue(PathUtils.hasPrefix("/a/b/c", "/a/b/c"));
    Assert.assertFalse(PathUtils.hasPrefix("/", "/a"));
    Assert.assertFalse(PathUtils.hasPrefix("/", "/a/b/c"));
    Assert.assertFalse(PathUtils.hasPrefix("/a", "/a/b/c"));
    Assert.assertFalse(PathUtils.hasPrefix("/a/b", "/a/b/c"));
    Assert.assertFalse(PathUtils.hasPrefix("/a/b/c", "/aa"));
    Assert.assertFalse(PathUtils.hasPrefix("/a/b/c", "/a/bb"));
    Assert.assertFalse(PathUtils.hasPrefix("/a/b/c", "/a/b/cc"));
    Assert.assertFalse(PathUtils.hasPrefix("/aa/b/c", "/a"));
    Assert.assertFalse(PathUtils.hasPrefix("/a/bb/c", "/a/b"));
    Assert.assertFalse(PathUtils.hasPrefix("/a/b/cc", "/a/b/c"));
  }

  /**
   * Tests the {@link PathUtils#isRoot(String)} method.
   */
  @Test
  public void isRoot() throws InvalidPathException {
    // check a path that is non-root
    Assert.assertFalse(PathUtils.isRoot("/foo/bar"));
    Assert.assertFalse(PathUtils.isRoot("/foo/bar/"));
    Assert.assertFalse(PathUtils.isRoot("/foo/./bar/"));
    Assert.assertFalse(PathUtils.isRoot("/foo/././bar/"));
    Assert.assertFalse(PathUtils.isRoot("/foo/../bar"));
    Assert.assertFalse(PathUtils.isRoot("/foo/../bar/"));

    // check a path that is root
    Assert.assertTrue(PathUtils.isRoot("/"));
    Assert.assertTrue(PathUtils.isRoot("/"));
    Assert.assertTrue(PathUtils.isRoot("/."));
    Assert.assertTrue(PathUtils.isRoot("/./"));
    Assert.assertTrue(PathUtils.isRoot("/foo/.."));
    Assert.assertTrue(PathUtils.isRoot("/foo/../"));
  }

  /**
   * Tests the {@link PathUtils#temporaryFileName(long, String)} method.
   */
  @Test
  public void temporaryFileName() {
    Assert.assertEquals(PathUtils.temporaryFileName(1, "/"),
        PathUtils.temporaryFileName(1, "/"));
    Assert.assertNotEquals(PathUtils.temporaryFileName(1, "/"),
        PathUtils.temporaryFileName(2, "/"));
    Assert.assertNotEquals(PathUtils.temporaryFileName(1, "/"),
        PathUtils.temporaryFileName(1, "/a"));
  }

  @Test
  public void getPermanentFileName() {
    Assert.assertEquals("/", PathUtils.getPermanentFileName(PathUtils.temporaryFileName(1, "/")));
    Assert.assertEquals("/",
        PathUtils.getPermanentFileName(PathUtils.temporaryFileName(0xFFFFFFFFFFFFFFFFL, "/")));
    Assert.assertEquals("/foo.alluxio.0x0123456789ABCDEF.tmp", PathUtils
        .getPermanentFileName(PathUtils.temporaryFileName(14324,
            "/foo.alluxio.0x0123456789ABCDEF.tmp")));
  }

  /**
   * Test the {@link PathUtils#isTemporaryFileName(String)} method.
   */
  @Test
  public void isTemporaryFileName() {
    Assert.assertTrue(PathUtils.isTemporaryFileName(PathUtils.temporaryFileName(0, "/")));
    Assert.assertTrue(
        PathUtils.isTemporaryFileName(PathUtils.temporaryFileName(0xFFFFFFFFFFFFFFFFL, "/")));
    Assert.assertTrue(PathUtils.isTemporaryFileName("foo.alluxio.0x0123456789ABCDEF.tmp"));
    Assert.assertFalse(PathUtils.isTemporaryFileName("foo.alluxio.0x      0123456789.tmp"));
    Assert.assertFalse(PathUtils.isTemporaryFileName("foo.alluxio.0x0123456789ABCDEFG.tmp"));
    Assert.assertFalse(PathUtils.isTemporaryFileName("foo.alluxio.0x0123456789ABCDE.tmp"));
    Assert.assertFalse(PathUtils.isTemporaryFileName("foo.0x0123456789ABCDEFG.tmp"));
    Assert.assertFalse(PathUtils.isTemporaryFileName("alluxio.0x0123456789ABCDEFG"));
  }

  /**
   * Tests the {@link PathUtils#uniqPath()} method.
   */
  @Test
  public void uniqPath() {
    Assert.assertNotEquals(PathUtils.uniqPath(), PathUtils.uniqPath());
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
    LinkedList<String> invalidPaths = new LinkedList<>();
    invalidPaths.add(null);
    invalidPaths.add("");
    invalidPaths.add(" /");
    invalidPaths.add("/ ");
    for (String invalidPath : invalidPaths) {
      try {
        PathUtils.validatePath(invalidPath);
        Assert.fail("validatePath(" + invalidPath + ") did not fail");
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
    Assert.assertEquals("/", PathUtils.normalizePath("", "/"));
    Assert.assertEquals("/", PathUtils.normalizePath("/", "/"));
    Assert.assertEquals("/foo/bar/", PathUtils.normalizePath("/foo/bar", "/"));
    Assert.assertEquals("/foo/bar/", PathUtils.normalizePath("/foo/bar/", "/"));
    Assert.assertEquals("/foo/bar//", PathUtils.normalizePath("/foo/bar//", "/"));
    Assert.assertEquals("/foo/bar%", PathUtils.normalizePath("/foo/bar", "%"));
  }
}
