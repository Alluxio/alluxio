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

package alluxio;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import alluxio.util.OSUtils;

/**
 * Unit tests for alluxio.TachyonURITest
 */
public class TachyonURITest {

  private static final boolean WINDOWS = OSUtils.isWindows();

  /**
   * Tests the {@link TachyonURI#TachyonURI(String)} constructor for basic paths.
   */
  @Test
  public void basicTest1() {
    TachyonURI uri = new TachyonURI("alluxio://localhost:19998/xy z/a b c");
    Assert.assertEquals("localhost:19998", uri.getAuthority());
    Assert.assertEquals(2, uri.getDepth());
    Assert.assertEquals("localhost", uri.getHost());
    Assert.assertEquals("a b c", uri.getName());
    Assert.assertEquals("alluxio://localhost:19998/xy z", uri.getParent().toString());
    Assert.assertEquals("alluxio://localhost:19998/", uri.getParent().getParent().toString());
    Assert.assertEquals("/xy z/a b c", uri.getPath());
    Assert.assertEquals(19998, uri.getPort());
    Assert.assertEquals("tachyon", uri.getScheme());
    Assert.assertTrue(uri.hasAuthority());
    Assert.assertTrue(uri.hasScheme());
    Assert.assertTrue(uri.isAbsolute());
    Assert.assertTrue(uri.isPathAbsolute());
    Assert.assertEquals("alluxio://localhost:19998/xy z/a b c/d", uri.join("/d").toString());
    Assert.assertEquals("alluxio://localhost:19998/xy z/a b c/d", uri.join(new TachyonURI("/d"))
        .toString());
    Assert.assertEquals("alluxio://localhost:19998/xy z/a b c", uri.toString());
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String)} constructor for basic paths.
   */
  @Test
  public void basicTest2() {
    TachyonURI uri = new TachyonURI("hdfs://localhost/xy z/a b c");
    Assert.assertEquals("localhost", uri.getAuthority());
    Assert.assertEquals(2, uri.getDepth());
    Assert.assertEquals("localhost", uri.getHost());
    Assert.assertEquals("a b c", uri.getName());
    Assert.assertEquals("hdfs://localhost/xy z", uri.getParent().toString());
    Assert.assertEquals("hdfs://localhost/", uri.getParent().getParent().toString());
    Assert.assertEquals("/xy z/a b c", uri.getPath());
    Assert.assertEquals(-1, uri.getPort());
    Assert.assertEquals("hdfs", uri.getScheme());
    Assert.assertTrue(uri.hasAuthority());
    Assert.assertTrue(uri.hasScheme());
    Assert.assertTrue(uri.isAbsolute());
    Assert.assertTrue(uri.isPathAbsolute());
    Assert.assertEquals("hdfs://localhost/xy z/a b c/d", uri.join("/d").toString());
    Assert.assertEquals("hdfs://localhost/xy z/a b c/d", uri.join(new TachyonURI("/d"))
        .toString());
    Assert.assertEquals("hdfs://localhost/xy z/a b c", uri.toString());
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String)} constructor for basic paths.
   */
  @Test
  public void basicTests() {
    String[] strs =
        new String[] {"alluxio://localhost:19998/xyz/abc", "hdfs://localhost:19998/xyz/abc",
            "s3://localhost:19998/xyz/abc", "alluxio://localhost:19998/xy z/a b c",
            "hdfs://localhost:19998/xy z/a b c", "s3://localhost:19998/xy z/a b c"};
    for (String str : strs) {
      TachyonURI uri = new TachyonURI(str);
      Assert.assertEquals(str, uri.toString());
      Assert.assertEquals(2, uri.getDepth());
      Assert.assertEquals("localhost", uri.getHost());
      Assert.assertEquals(19998, uri.getPort());
    }
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String)} constructor for an empty URI.
   */
  @Test
  public void emptyURITest() {
    TachyonURI uri = new TachyonURI("");
    Assert.assertEquals(null, uri.getAuthority());
    Assert.assertEquals(0, uri.getDepth());
    Assert.assertEquals(null, uri.getHost());
    Assert.assertEquals("", uri.getName());
    Assert.assertEquals("", uri.getPath());
    Assert.assertEquals(-1, uri.getPort());
    Assert.assertEquals(null, uri.getScheme());
    Assert.assertFalse(uri.hasAuthority());
    Assert.assertFalse(uri.hasScheme());
    Assert.assertFalse(uri.isAbsolute());
    Assert.assertFalse(uri.isPathAbsolute());
    Assert.assertEquals("/d", uri.join("/d").toString());
    Assert.assertEquals("/d", uri.join(new TachyonURI("/d")).toString());
    Assert.assertEquals("", uri.toString());
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String, String, String)} constructor to build an URI
   * from its different components.
   */
  @Test
  public void constructFromComponentsTests() {
    String scheme = "tachyon";
    String authority = "127.0.0.1:90909";
    String path = "/a/../b/c.txt";
    String absPath = "/b/c.txt";

    TachyonURI uri0 = new TachyonURI(null, null, path);
    Assert.assertEquals(absPath, uri0.toString());

    TachyonURI uri1 = new TachyonURI(scheme, null, path);
    Assert.assertEquals(scheme + "://" + absPath, uri1.toString());

    TachyonURI uri2 = new TachyonURI(scheme, authority, path);
    Assert.assertEquals(scheme + "://" + authority + absPath, uri2.toString());

    TachyonURI uri3 = new TachyonURI(null, authority, path);
    Assert.assertEquals("//" + authority + absPath, uri3.toString());
  }

  /**
   * Tests to resolve a child TachyonURI against a parent TachyonURI.
   */
  @Test
  public void constructFromParentAndChildTests() {
    testParentChild(".", ".", ".");
    testParentChild("/", "/", ".");
    testParentChild("/", ".", "/");
    testParentChild("hdfs://localhost:8080/a/b/d.txt", "hdfs://localhost:8080/a/b/c.txt",
        "../d.txt");
    testParentChild("/foo/bar", "/foo", "bar");
    testParentChild("/foo/bar/baz", "/foo/bar", "baz");
    testParentChild("/foo/bar/baz", "/foo", "bar/baz");
    testParentChild("foo/bar", "foo", "bar");
    testParentChild("foo/bar/baz", "foo", "bar/baz");
    testParentChild("foo/bar/baz", "foo/bar", "baz");
    testParentChild("/foo", "/bar", "/foo");
    testParentChild("c:/foo", "/bar", "c:/foo");
    testParentChild("c:/foo", "d:/bar", "c:/foo");
    testParentChild("/foo/bar/baz/boo", "/foo/bar", "baz/boo");
    testParentChild("foo/bar/baz/bud", "foo/bar/", "baz/bud");
    testParentChild("/boo/bud", "/foo/bar", "../../boo/bud");
    testParentChild("boo/bud", "foo/bar", "../../boo/bud");
    testParentChild("/foo/boo/bud", "/foo/bar/baz", "../../boo/bud");
    testParentChild("foo/boo/bud", "foo/bar/baz", "../../boo/bud");
    testParentChild("../../../../boo/bud", "../../", "../../boo/bud");
    testParentChild("../../../../boo/bud", "../../foo", "../../../boo/bud");
    testParentChild("../../foo/boo/bud", "../../foo/bar", "../boo/bud");
    testParentChild("", "foo/bar/baz", "../../..");
    testParentChild("../..", "foo/bar/baz", "../../../../..");

    testParentChild("foo://bar boo:80/.", "foo://bar boo:80/.", ".");
    testParentChild("foo://bar boo:80/", "foo://bar boo:80/", ".");
    testParentChild("foo://bar boo:80/", "foo://bar boo:80/.", "/");
    testParentChild("foo://bar boo:80/foo", "foo://bar boo:80/", "foo");
    testParentChild("foo://bar boo:80/foo/bar", "foo://bar boo:80/foo", "bar");
    testParentChild("foo://bar boo:80/foo/bar/baz", "foo://bar boo:80/foo/bar", "baz");
    testParentChild("foo://bar boo:80/foo", "foo://bar boo:80/.", "foo");
    testParentChild("foo://bar boo:80/foo/bar/baz", "foo://bar boo:80/foo", "bar/baz");
    testParentChild("foo://bar boo:80/foo/bar/baz", "foo://bar boo:80/foo/bar", "baz");
    testParentChild("foo://bar boo:80/foo", "foo://bar boo:80/bar", "/foo");
    testParentChild("foo://bar boo:80/c:/foo", "foo://bar boo:80/bar", "c:/foo");
    testParentChild("foo://bar boo:80/c:/foo", "foo://bar boo:80/d:/bar", "c:/foo");
    testParentChild("foo://bar boo:80/c:/foo", "foo://bar boo:80/d:/bar",
        "foo://bar boo:80/c:/foo");
  }

  /**
   * Tests the {@link TachyonURI#compareTo(TachyonURI)} method.
   */
  @Test
  public void compareToTests() {
    TachyonURI[] uris =
        new TachyonURI[] {new TachyonURI("file://127.0.0.0:8081/a/b/c.txt"),
            new TachyonURI("glusterfs://127.0.0.0:8081/a/b/c.txt"),
            new TachyonURI("hdfs://127.0.0.0:8081/a/b/c.txt"),
            new TachyonURI("hdfs://127.0.0.1:8081/a/b/c.txt"),
            new TachyonURI("hdfs://127.0.0.1:8081/a/b/d.txt"),
            new TachyonURI("hdfs://127.0.0.1:8081/a/c/c.txt"),
            new TachyonURI("hdfs://127.0.0.1:8082/a/c/c.txt"),
            new TachyonURI("hdfs://localhost:8080/a/b/c.txt"),
            new TachyonURI("s3://localhost:8080/a/b/c.txt")};

    for (int i = 0; i < uris.length - 1; i ++) {
      Assert.assertTrue(uris[i].compareTo(uris[i + 1]) < 0);
      Assert.assertTrue(uris[i + 1].compareTo(uris[i]) > 0);
      Assert.assertEquals(0, uris[i].compareTo(uris[i]));
    }
  }

  /**
   * Tests the {@link TachyonURI#equals(Object)} method.
   */
  @Test
  public void equalsTests() {
    Assert.assertFalse(new TachyonURI("alluxio://127.0.0.1:8080/a/b/c.txt").equals(new TachyonURI(
        "alluxio://localhost:8080/a/b/c.txt")));

    TachyonURI[] uriFromDifferentConstructor =
        new TachyonURI[] {new TachyonURI("alluxio://127.0.0.1:8080/a/b/c.txt"),
            new TachyonURI("tachyon", "127.0.0.1:8080", "/a/b/c.txt"),
            new TachyonURI(
                new TachyonURI("alluxio://127.0.0.1:8080/a"), new TachyonURI("b/c.txt"))};
    for (int i = 0; i < uriFromDifferentConstructor.length - 1; i ++) {
      Assert.assertTrue(uriFromDifferentConstructor[i].equals(uriFromDifferentConstructor[i + 1]));
    }
  }

  /**
   * Tests the {@link TachyonURI#getAuthority()} method.
   */
  @Test
  public void getAuthorityTests() {
    String[] authorities =
        new String[] {"localhost", "localhost:8080", "127.0.0.1", "127.0.0.1:8080", "localhost",
            null};
    for (String authority : authorities) {
      TachyonURI uri = new TachyonURI("file", authority, "/a/b");
      Assert.assertEquals(authority, uri.getAuthority());
    }

    Assert.assertEquals(null, new TachyonURI("file", "", "/b/c").getAuthority());
    Assert.assertEquals(null, new TachyonURI("file", null, "/b/c").getAuthority());
    Assert.assertEquals(null, new TachyonURI("file:///b/c").getAuthority());
  }

  /**
   * Tests the {@link TachyonURI#getDepth()} method.
   */
  @Test
  public void getDepthTests() {
    Assert.assertEquals(0, new TachyonURI("").getDepth());
    Assert.assertEquals(0, new TachyonURI(".").getDepth());
    Assert.assertEquals(0, new TachyonURI("/").getDepth());
    Assert.assertEquals(1, new TachyonURI("/a").getDepth());
    Assert.assertEquals(3, new TachyonURI("/a/b/c.txt").getDepth());
    Assert.assertEquals(2, new TachyonURI("/a/b/").getDepth());
    Assert.assertEquals(2, new TachyonURI("a\\b").getDepth());
    Assert.assertEquals(1, new TachyonURI("C:\\a").getDepth());
    Assert.assertEquals(1, new TachyonURI("C:\\\\a").getDepth());
    Assert.assertEquals(0, new TachyonURI("C:\\\\").getDepth());
    Assert.assertEquals(0, new TachyonURI("alluxio://localhost:1998/").getDepth());
    Assert.assertEquals(1, new TachyonURI("alluxio://localhost:1998/a").getDepth());
    Assert.assertEquals(2, new TachyonURI("alluxio://localhost:1998/a/b.txt").getDepth());
  }

  /**
   * Tests the {@link TachyonURI#getHost()} method.
   */
  @Test
  public void getHostTests() {
    Assert.assertEquals(null, new TachyonURI("/").getHost());
    Assert.assertEquals(null, new TachyonURI("file", "", "/a/b.txt").getHost());
    Assert.assertEquals(null, new TachyonURI("file", null, "/a/b.txt").getHost());
    Assert.assertEquals("localhost", new TachyonURI("s3", "localhost", "/a/b.txt").getHost());
    Assert.assertEquals("localhost", new TachyonURI("s3", "localhost:8080", "/a/b.txt").getHost());
    Assert.assertEquals("127.0.0.1", new TachyonURI("s3", "127.0.0.1", "/a/b.txt").getHost());
    Assert.assertEquals("127.0.0.1", new TachyonURI("s3", "127.0.0.1:8080", "/a/b.txt").getHost());
  }

  /**
   * Tests the {@link TachyonURI#getName()} method.
   */
  @Test
  public void getNameTests() {
    Assert.assertEquals("", new TachyonURI("/").getName());
    Assert.assertEquals("", new TachyonURI("alluxio://localhost/").getName());
    Assert.assertEquals("", new TachyonURI("alluxio:/").getName());
    Assert.assertEquals("a", new TachyonURI("alluxio:/a/").getName());
    Assert.assertEquals("a.txt", new TachyonURI("alluxio:/a.txt/").getName());
    Assert.assertEquals(" b.txt", new TachyonURI("alluxio:/a/ b.txt").getName());
    Assert.assertEquals("a.txt", new TachyonURI("/a/a.txt").getName());
  }

  /**
   * Tests the {@link TachyonURI#getParent()} method.
   */
  @Test
  public void getParentTests() {
    Assert.assertEquals(null, new TachyonURI("/").getParent());
    Assert.assertEquals(null,
        new TachyonURI("alluxio://localhost/").getParent());
    Assert.assertEquals(new TachyonURI("alluxio://localhost/"),
        new TachyonURI("alluxio://localhost/a").getParent());
    Assert.assertEquals(new TachyonURI("/a"), new TachyonURI("/a/b/../c").getParent());
    Assert.assertEquals(new TachyonURI("alluxio:/a"),
        new TachyonURI("alluxio:/a/b/../c").getParent());
    Assert.assertEquals(new TachyonURI("alluxio://localhost:80/a"), new TachyonURI(
        "alluxio://localhost:80/a/b/../c").getParent());
  }

  /**
   * Tests the {@link TachyonURI#getPath()} method.
   */
  @Test
  public void getPathTests() {
    Assert.assertEquals("/", new TachyonURI("/").getPath());
    Assert.assertEquals("/", new TachyonURI("alluxio:/").getPath());
    Assert.assertEquals("/", new TachyonURI("alluxio://localhost:80/").getPath());
    Assert.assertEquals("/a.txt", new TachyonURI("alluxio://localhost:80/a.txt").getPath());
    Assert.assertEquals("/b", new TachyonURI("alluxio://localhost:80/a/../b").getPath());
    Assert.assertEquals("/b", new TachyonURI("alluxio://localhost:80/a/c/../../b").getPath());
    Assert.assertEquals("/a/b", new TachyonURI("alluxio://localhost:80/a/./b").getPath());
  }

  /**
   * Tests the {@link TachyonURI#getPort()} method.
   */
  @Test
  public void getPortTests() {
    Assert.assertEquals(-1, new TachyonURI("/").getPort());
    Assert.assertEquals(-1, new TachyonURI("alluxio:/").getPort());
    Assert.assertEquals(-1, new TachyonURI("alluxio://127.0.0.1/").getPort());
    Assert.assertEquals(-1, new TachyonURI("alluxio://localhost/").getPort());
    Assert.assertEquals(8080, new TachyonURI("alluxio://localhost:8080/").getPort());
    Assert.assertEquals(8080, new TachyonURI("alluxio://127.0.0.1:8080/").getPort());
  }

  /**
   * Tests the {@link TachyonURI#getScheme()} method.
   */
  @Test
  public void getSchemeTests() {
    Assert.assertEquals(null, new TachyonURI("/").getScheme());
    Assert.assertEquals("file", new TachyonURI("file:/").getScheme());
    Assert.assertEquals("file", new TachyonURI("file://localhost/").getScheme());
    Assert.assertEquals("alluxio-ft", new TachyonURI("alluxio-ft://localhost/").getScheme());
    Assert.assertEquals("s3", new TachyonURI("s3://localhost/").getScheme());
    Assert.assertEquals("tachyon", new TachyonURI("alluxio://localhost/").getScheme());
    Assert.assertEquals("hdfs", new TachyonURI("hdfs://localhost/").getScheme());
    Assert.assertEquals("glusterfs", new TachyonURI("glusterfs://localhost/").getScheme());
  }

  /**
   * Tests the {@link TachyonURI#hasAuthority()} method.
   */
  @Test
  public void hasAuthorityTests() {
    Assert.assertFalse(new TachyonURI("/").hasAuthority());
    Assert.assertFalse(new TachyonURI("file:/").hasAuthority());
    Assert.assertFalse(new TachyonURI("file:///test").hasAuthority());
    Assert.assertTrue(new TachyonURI("file://localhost/").hasAuthority());
    Assert.assertTrue(new TachyonURI("file://localhost:8080/").hasAuthority());
    Assert.assertTrue(new TachyonURI(null, "localhost:8080", "/").hasAuthority());
    Assert.assertTrue(new TachyonURI(null, "localhost", "/").hasAuthority());
  }

  /**
   * Tests the {@link TachyonURI#hasScheme()} method.
   */
  @Test
  public void hasScheme() {
    Assert.assertFalse(new TachyonURI("/").hasScheme());
    Assert.assertTrue(new TachyonURI("file:/").hasScheme());
    Assert.assertTrue(new TachyonURI("file://localhost/").hasScheme());
    Assert.assertTrue(new TachyonURI("file://localhost:8080/").hasScheme());
    Assert.assertFalse(new TachyonURI("//localhost:8080/").hasScheme());
  }

  /**
   * Tests the {@link TachyonURI#isAbsolute()} method.
   */
  @Test
  public void isAbsoluteTests() {
    Assert.assertTrue(new TachyonURI("file:/a").isAbsolute());
    Assert.assertTrue(new TachyonURI("file://localhost/a").isAbsolute());
    Assert.assertFalse(new TachyonURI("//localhost/a").isAbsolute());
    Assert.assertFalse(new TachyonURI("//localhost/").isAbsolute());
    Assert.assertFalse(new TachyonURI("/").isAbsolute());
  }

  /**
   * Tests the {@link TachyonURI#isPathAbsolute()} method.
   */
  @Test
  public void isPathAbsoluteTests() {
    Assert.assertFalse(new TachyonURI(".").isPathAbsolute());
    Assert.assertTrue(new TachyonURI("/").isPathAbsolute());
    Assert.assertTrue(new TachyonURI("file:/").isPathAbsolute());
    Assert.assertTrue(new TachyonURI("file://localhost/").isPathAbsolute());
    Assert.assertTrue(new TachyonURI("file://localhost/a/b").isPathAbsolute());
    Assert.assertFalse(new TachyonURI("a/b").isPathAbsolute());
    Assert.assertTrue(new TachyonURI("C:\\\\a\\b").isPathAbsolute());
  }

  /**
   * Tests the {@link TachyonURI#isRoot()} method.
   */
  @Test
  public void isRootTests() {
    Assert.assertFalse(new TachyonURI(".").isRoot());
    Assert.assertTrue(new TachyonURI("/").isRoot());
    Assert.assertTrue(new TachyonURI("file:/").isRoot());
    Assert.assertTrue(new TachyonURI("alluxio://localhost:19998").isRoot());
    Assert.assertTrue(new TachyonURI("alluxio://localhost:19998/").isRoot());
    Assert.assertTrue(new TachyonURI("hdfs://localhost:19998").isRoot());
    Assert.assertTrue(new TachyonURI("hdfs://localhost:19998/").isRoot());
    Assert.assertTrue(new TachyonURI("file://localhost/").isRoot());
    Assert.assertFalse(new TachyonURI("file://localhost/a/b").isRoot());
    Assert.assertFalse(new TachyonURI("a/b").isRoot());
  }

  /**
   * Tests the {@link TachyonURI#join(String)} and {@link TachyonURI#join(TachyonURI)} methods.
   */
  @Test
  public void joinTests() {
    Assert.assertEquals(new TachyonURI("/a"), new TachyonURI("/").join("a"));
    Assert.assertEquals(new TachyonURI("/a"), new TachyonURI("/").join(new TachyonURI("a")));
    Assert.assertEquals(new TachyonURI("/a/b"), new TachyonURI("/a").join(new TachyonURI("b")));
    Assert.assertEquals(new TachyonURI("a/b"), new TachyonURI("a").join(new TachyonURI("b")));
    Assert.assertEquals(new TachyonURI("/a/c"),
        new TachyonURI("/a").join(new TachyonURI("b/../c")));
    Assert.assertEquals(new TachyonURI("a/b.txt"),
        new TachyonURI("a").join(new TachyonURI("/b.txt")));
    Assert.assertEquals(new TachyonURI("a/b.txt"),
        new TachyonURI("a").join(new TachyonURI("/c/../b.txt")));
    Assert.assertEquals(new TachyonURI("alluxio:/a/b.txt"),
        new TachyonURI("alluxio:/a").join("/b.txt"));
    Assert.assertEquals(new TachyonURI("alluxio:/a/b.txt"),
        new TachyonURI("alluxio:/a/c.txt").join(new TachyonURI("/../b.txt")));
    Assert.assertEquals(new TachyonURI("C:\\\\a\\b"),
        new TachyonURI("C:\\\\a").join(new TachyonURI("\\b")));

    final String pathWithSpecialChar = "����,��b����$o����[| =B����";
    Assert.assertEquals(new TachyonURI("/" + pathWithSpecialChar),
            new TachyonURI("/").join(pathWithSpecialChar));

    final String pathWithSpecialCharAndColon = "����,��b����$o����[| =B��:��";
    Assert.assertEquals(new TachyonURI("/" + pathWithSpecialCharAndColon),
        new TachyonURI("/").join(pathWithSpecialCharAndColon));
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String)} constructor to work with file URIs
   * appropriately.
   */
  @Test
  public void fileUriTests() {
    TachyonURI uri = new TachyonURI("file:///foo/bar");
    Assert.assertFalse(uri.hasAuthority());
    Assert.assertEquals("/foo/bar", uri.getPath());
    Assert.assertEquals("file:///foo/bar", uri.toString());
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String)} constructor to work with Windows paths
   * appropriately.
   */
  @Test
  public void windowsPathTests() {
    Assume.assumeTrue(WINDOWS);

    TachyonURI uri = new TachyonURI("C:\\foo\\bar");
    Assert.assertFalse(uri.hasAuthority());
    Assert.assertEquals("/C:/foo/bar", uri.getPath());
    Assert.assertEquals("C:/foo/bar", uri.toString());
  }

  /**
   * Tests the {@link TachyonURI#toString()} method to work with paths appropriately.
   */
  @Test
  public void toStringTests() {
    String[] uris =
        new String[] {"/", "/a", "/a/ b", "alluxio://a/b/c d.txt",
            "alluxio://localhost:8080/a/b.txt", "foo", "foo/bar", "/foo/bar#boo", "foo/bar#boo",
            "file:///foo/bar"};
    for (String uri : uris) {
      TachyonURI turi = new TachyonURI(uri);
      Assert.assertEquals(uri, turi.toString());
    }

    Assert.assertEquals("", new TachyonURI(".").toString());
    Assert.assertEquals("file:///a", new TachyonURI("file:///a").toString());
    Assert.assertEquals("file:///a", new TachyonURI("file", null, "/a").toString());
  }

  /**
   * Tests the {@link TachyonURI#toString()} method to work with Windows paths appropriately.
   */
  @Test
  public void toStringWindowsTests() {
    Assume.assumeTrue(WINDOWS);

    String[] uris =
        new String[] { "c:/", "c:/foo/bar", "C:/foo/bar#boo", "C:/foo/ bar" };
    for (String uri : uris) {
      TachyonURI turi = new TachyonURI(uri);
      Assert.assertEquals(uri, turi.toString());
    }

    Assert.assertEquals("C:/", new TachyonURI("C:\\\\").toString());
    Assert.assertEquals("C:/a/b.txt", new TachyonURI("C:\\\\a\\b.txt").toString());
  }

  /**
   * Tests the {@link TachyonURI#toString()} method to normalize paths.
   */
  @Test
  public void normalizeTests() {
    Assert.assertEquals("/", new TachyonURI("//").toString());
    Assert.assertEquals("/foo", new TachyonURI("/foo/").toString());
    Assert.assertEquals("/foo", new TachyonURI("/foo/").toString());
    Assert.assertEquals("foo", new TachyonURI("foo/").toString());
    Assert.assertEquals("foo", new TachyonURI("foo//").toString());
    Assert.assertEquals("foo/bar", new TachyonURI("foo//bar").toString());

    Assert.assertEquals("foo/boo", new TachyonURI("foo/bar/..//boo").toString());
    Assert.assertEquals("foo/boo/baz", new TachyonURI("foo/bar/..//boo/./baz").toString());
    Assert.assertEquals("../foo/boo", new TachyonURI("../foo/bar/..//boo").toString());
    Assert.assertEquals("/../foo/boo", new TachyonURI("/.././foo/boo").toString());
    Assert.assertEquals("foo/boo", new TachyonURI("./foo/boo").toString());

    Assert.assertEquals("foo://bar boo:8080/abc/c",
        new TachyonURI("foo://bar boo:8080/abc///c").toString());
  }

  /**
   * Tests the {@link TachyonURI#toString()} method to normalize Windows paths.
   */
  @Test
  public void normalizeWindowsTests() {
    Assume.assumeTrue(WINDOWS);

    Assert.assertEquals("c:/a/b", new TachyonURI("c:\\a\\b").toString());
    Assert.assertEquals("c:/a/c", new TachyonURI("c:\\a\\b\\..\\c").toString());
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String, String, String)} constructor to throw an
   * exception in case an empty path was provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void constructFromEmptyPathTest2() {
    new TachyonURI(null, null, null);
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String, String, String)} constructor to throw an
   * exception in case an empty path was provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void constructFromEmptyPathTest3() {
    new TachyonURI("file", null, "");
  }

  /**
   * Tests the {@link TachyonURI#TachyonURI(String)} constructor to throw an exception in case an
   * invalid URI was provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void invalidURISyntaxTest() {
    new TachyonURI("://localhost:8080/a");
  }

  private void testParentChild(String target, String parent, String child) {
    if (target.length() > 0) {
      Assert.assertEquals(new TachyonURI(target), new TachyonURI(new TachyonURI(parent),
          new TachyonURI(child)));
    } else {
      Assert.assertEquals(target,
          new TachyonURI(new TachyonURI(parent), new TachyonURI(child)).toString());
    }
  }

  /**
   * Tests the {@link TachyonURI#getLeadingPath(int)} method.
   */
  @Test
  public void getLeadingPathTest() {
    Assert.assertEquals("/",      new TachyonURI("/a/b/c/").getLeadingPath(0));
    Assert.assertEquals("/a",     new TachyonURI("/a/b/c/").getLeadingPath(1));
    Assert.assertEquals("/a/b",   new TachyonURI("/a/b/c/").getLeadingPath(2));
    Assert.assertEquals("/a/b/c", new TachyonURI("/a/b/c/").getLeadingPath(3));
    Assert.assertEquals(null,     new TachyonURI("/a/b/c/").getLeadingPath(4));

    Assert.assertEquals("/",      new TachyonURI("/").getLeadingPath(0));

    Assert.assertEquals("",       new TachyonURI("").getLeadingPath(0));
    Assert.assertEquals(null,     new TachyonURI("").getLeadingPath(1));
    Assert.assertEquals("",       new TachyonURI(".").getLeadingPath(0));
    Assert.assertEquals(null,     new TachyonURI(".").getLeadingPath(1));
    Assert.assertEquals("a/b",    new TachyonURI("a/b/c").getLeadingPath(1));
  }
}
