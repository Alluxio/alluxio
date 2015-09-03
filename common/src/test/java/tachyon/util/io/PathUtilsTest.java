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

package tachyon.util.io;

import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.thrift.InvalidPathException;

public class PathUtilsTest {
  @Rule
  public final ExpectedException mException = ExpectedException.none();

  @Test
  public void getPathComponentsNoExceptionTest() throws InvalidPathException {
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

  @Test
  public void getPathComponentsExceptionTest() throws InvalidPathException {
    mException.expect(InvalidPathException.class);
    PathUtils.getPathComponents("/\\   foo / bar");
  }

  @Test
  public void concatPathTest() {
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

  @Test
  public void cleanPathNoExceptionTest() throws InvalidPathException {
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
    // TODO instead of returning null, throw InvalidPathException
    Assert.assertNull(PathUtils.cleanPath("/foo/bar/../../.."));
  }

  @Test
  public void cleanPathExceptionTest() throws InvalidPathException {
    mException.expect(InvalidPathException.class);
    Assert.assertEquals("/foo/bar", PathUtils.cleanPath("/\\   foo / bar"));
  }

  @Test
  public void getParentTest() throws InvalidPathException {
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

  @Test
  public void isRootTest() throws InvalidPathException {
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

  @Test
  public void validatePathTest() throws InvalidPathException {
    // check valid paths
    PathUtils.validatePath("/foo/bar");
    PathUtils.validatePath("/foo/bar/");
    PathUtils.validatePath("/foo/./bar/");
    PathUtils.validatePath("/foo/././bar/");
    PathUtils.validatePath("/foo/../bar");
    PathUtils.validatePath("/foo/../bar/");

    // check invalid paths
    LinkedList<String> invalidPaths = new LinkedList<String>();
    invalidPaths.add(null);
    invalidPaths.add("");
    invalidPaths.add(" /");
    invalidPaths.add("/ ");
    for (String invalidPath : invalidPaths) {
      try {
        PathUtils.validatePath(invalidPath);
        Assert.fail("validatePath(" + invalidPath + ") did not fail");
      } catch (InvalidPathException ipe) {
        // this is expected
      }
    }
  }

  @Test
  public void uniqPathTest() {
    Assert.assertNotEquals(PathUtils.uniqPath(), PathUtils.uniqPath());
  }
}
