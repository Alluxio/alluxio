/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import alluxio.util.OSUtils;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link AlluxioURI}.
 */
public class AlluxioURITest {

  private static final boolean WINDOWS = OSUtils.isWindows();

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor for basic paths.
   */
  @Test
  public void basicTest1() {
    AlluxioURI uri = new AlluxioURI("alluxio://localhost:19998/xy z/a b c");
    Assert.assertEquals("localhost:19998", uri.getAuthority());
    Assert.assertEquals(2, uri.getDepth());
    Assert.assertEquals("localhost", uri.getHost());
    Assert.assertEquals("a b c", uri.getName());
    Assert.assertEquals("alluxio://localhost:19998/xy z", uri.getParent().toString());
    Assert.assertEquals("alluxio://localhost:19998/", uri.getParent().getParent().toString());
    Assert.assertEquals("/xy z/a b c", uri.getPath());
    Assert.assertEquals(19998, uri.getPort());
    Assert.assertEquals("alluxio", uri.getScheme());
    Assert.assertTrue(uri.hasAuthority());
    Assert.assertTrue(uri.hasScheme());
    Assert.assertTrue(uri.isAbsolute());
    Assert.assertTrue(uri.isPathAbsolute());
    Assert.assertEquals("alluxio://localhost:19998/xy z/a b c/d", uri.join("/d").toString());
    Assert.assertEquals("alluxio://localhost:19998/xy z/a b c/d", uri.join(new AlluxioURI("/d"))
        .toString());
    Assert.assertEquals("alluxio://localhost:19998/xy z/a b c", uri.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor for basic paths.
   */
  @Test
  public void basicTest2() {
    AlluxioURI uri = new AlluxioURI("hdfs://localhost/xy z/a b c");
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
    Assert.assertEquals("hdfs://localhost/xy z/a b c/d", uri.join(new AlluxioURI("/d"))
        .toString());
    Assert.assertEquals("hdfs://localhost/xy z/a b c", uri.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor with query parameter.
   */
  @Test
  public void basicConstructorQueryTest() {
    /**
     * Some encodings:
     * '&' -> %26
     * '=' -> %3D
     * ' ' -> %20
     * '%' -> %25
     * '+' -> %2B
     */
    String queryPart = "k1=v1&k2= spaces &k3=%3D%20escapes %20%25%26%2B&!@#$^*()-_=[]{};\"'<>,./";
    AlluxioURI uri = new AlluxioURI("hdfs://localhost/a?" + queryPart);
    Map<String, String> queryMap = uri.getQueryMap();
    Assert.assertEquals(4, queryMap.size());
    Assert.assertEquals("v1", queryMap.get("k1"));
    Assert.assertEquals(" spaces ", queryMap.get("k2"));
    Assert.assertEquals("= escapes  %&+", queryMap.get("k3"));
    Assert.assertEquals("[]{};\"'<>,./", queryMap.get("!@#$^*()-_"));
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor for basic paths.
   */
  @Test
  public void basicTests() {
    String[] strs =
        new String[] {"alluxio://localhost:19998/xyz/abc", "hdfs://localhost:19998/xyz/abc",
            "s3://localhost:19998/xyz/abc", "alluxio://localhost:19998/xy z/a b c",
            "hdfs://localhost:19998/xy z/a b c", "s3://localhost:19998/xy z/a b c"};
    for (String str : strs) {
      AlluxioURI uri = new AlluxioURI(str);
      Assert.assertEquals(str, uri.toString());
      Assert.assertEquals(2, uri.getDepth());
      Assert.assertEquals("localhost", uri.getHost());
      Assert.assertEquals(19998, uri.getPort());
    }
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor for an empty URI.
   */
  @Test
  public void emptyURITest() {
    AlluxioURI uri = new AlluxioURI("");
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
    Assert.assertEquals("/d", uri.join(new AlluxioURI("/d")).toString());
    Assert.assertEquals("", uri.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String, String, String)} constructor to build an URI
   * from its different components.
   */
  @Test
  public void constructFromComponentsTests() {
    String scheme = "alluxio";
    String authority = "127.0.0.1:90909";
    String path = "/a/../b/c.txt";
    String absPath = "/b/c.txt";

    AlluxioURI uri0 = new AlluxioURI(null, null, path);
    Assert.assertEquals(absPath, uri0.toString());

    AlluxioURI uri1 = new AlluxioURI(scheme, null, path);
    Assert.assertEquals(scheme + "://" + absPath, uri1.toString());

    AlluxioURI uri2 = new AlluxioURI(scheme, authority, path);
    Assert.assertEquals(scheme + "://" + authority + absPath, uri2.toString());

    AlluxioURI uri3 = new AlluxioURI(null, authority, path);
    Assert.assertEquals("//" + authority + absPath, uri3.toString());

    AlluxioURI uri4 = new AlluxioURI("scheme:part1", authority, path);
    Assert.assertEquals("scheme:part1://" + authority + absPath, uri4.toString());

    AlluxioURI uri5 = new AlluxioURI("scheme:part1:part2", authority, path);
    Assert.assertEquals("scheme:part1:part2://" + authority + absPath, uri5.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String, String, String, Map)} constructor to build an
   * URI from its different components with a query map.
   */
  @Test
  public void constructWithQueryMapTest() {
    String scheme = "alluxio";
    String authority = "host:1234";
    String path = "/a";
    Map<String, String> queryMap = new HashMap<>();
    queryMap.put("key", "123");
    queryMap.put(" k2 ", " v2 ");
    queryMap.put(" key: !*'();:@&=+$,/?#[]\"% ", " !*'();:@&=+$,/?#[]\"% ");
    queryMap.put(" key: %26 %3D %20 %25 %2B ", " %26 %3D %20 %25 %2B ");

    AlluxioURI uri1 = new AlluxioURI(scheme, authority, path, queryMap);
    AlluxioURI uri2 = new AlluxioURI(uri1.toString());
    Assert.assertEquals(queryMap, uri1.getQueryMap());
    Assert.assertEquals(uri1.getQueryMap(), uri2.getQueryMap());
    Map<String, String> m1 = uri1.getQueryMap();
    Map<String, String> m2 = uri2.getQueryMap();
  }

  /**
   * Tests to resolve a child {@link AlluxioURI} against a parent {@link AlluxioURI}.
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

    testParentChild("parent://host:1234/a/d.txt", "parent://host:1234/a", "d.txt");
    testParentChild("parent:part1://host:1234/a/d.txt", "parent:part1://host:1234/a", "d.txt");
    testParentChild("parent:part1:part2://host:1234/a/d.txt", "parent:part1:part2://host:1234/a",
        "d.txt");
    testParentChild("child://h:1/d.txt", "parent://host:1234/a", "child://h:1/d.txt");
    testParentChild("child:part1://h:1/d.txt", "parent://host:1234/a", "child:part1://h:1/d.txt");
    testParentChild("child:part1:part2://h:1/d.txt", "parent://host:1234/a",
        "child:part1:part2://h:1/d.txt");
  }

  /**
   * Tests the {@link AlluxioURI#compareTo(AlluxioURI)} method.
   */
  @Test
  public void compareToTests() {
    AlluxioURI[] uris =
        new AlluxioURI[] {new AlluxioURI("file://127.0.0.0:8081/a/b/c.txt"),
            new AlluxioURI("glusterfs://127.0.0.0:8081/a/b/c.txt"),
            new AlluxioURI("hdfs://127.0.0.0:8081/a/b/c.txt"),
            new AlluxioURI("hdfs://127.0.0.1:8081/a/b/c.txt"),
            new AlluxioURI("hdfs://127.0.0.1:8081/a/b/d.txt"),
            new AlluxioURI("hdfs://127.0.0.1:8081/a/c/c.txt"),
            new AlluxioURI("hdfs://127.0.0.1:8082/a/c/c.txt"),
            new AlluxioURI("hdfs://localhost:8080/a/b/c.txt"),
            new AlluxioURI("s3://localhost:8080/a/b/c.txt"),
            new AlluxioURI("scheme://localhost:8080/a.txt"),
            new AlluxioURI("scheme://localhost:8080/a.txt?a=a"),
            new AlluxioURI("scheme://localhost:8080/a.txt?b=b"),
            new AlluxioURI("scheme://localhost:8080/a.txt?c=c"),
            new AlluxioURI("scheme:scheme://localhost:8080/a.txt"),
            new AlluxioURI("scheme:scheme://localhost:8080/b.txt"),
            new AlluxioURI("scheme:schemeB://localhost:8080/a.txt"),
            new AlluxioURI("scheme:schemeB://localhost:8080/b.txt"),
            new AlluxioURI("schemeA:scheme://localhost:8080/a.txt"),
            new AlluxioURI("schemeA:scheme://localhost:8080/b.txt"),
            new AlluxioURI("schemeA:schemeB:schemeC://localhost:8080/a.txt"),
            new AlluxioURI("schemeA:schemeB:schemeC://localhost:8080/b.txt"),
            new AlluxioURI("schemeA:schemeB:schemeD://localhost:8080/a.txt"),
            new AlluxioURI("schemeA:schemeB:schemeD://localhost:8080/b.txt"),
            new AlluxioURI("schemeE:schemeB:schemeB://localhost:8080/a.txt"),
            new AlluxioURI("schemeE:schemeB:schemeB://localhost:8080/b.txt"),
        };

    for (int i = 0; i < uris.length - 1; i++) {
      Assert.assertTrue(uris[i].compareTo(uris[i + 1]) < 0);
      Assert.assertTrue(uris[i + 1].compareTo(uris[i]) > 0);
      Assert.assertEquals(0, uris[i].compareTo(uris[i]));
    }
  }

  /**
   * Tests the {@link AlluxioURI#equals(Object)} method.
   */
  @Test
  public void equalsTests() {
    Assert.assertFalse(new AlluxioURI("alluxio://127.0.0.1:8080/a/b/c.txt").equals(new AlluxioURI(
        "alluxio://localhost:8080/a/b/c.txt")));

    AlluxioURI[] uriFromDifferentConstructor =
        new AlluxioURI[] {new AlluxioURI("alluxio://127.0.0.1:8080/a/b/c.txt"),
            new AlluxioURI("alluxio", "127.0.0.1:8080", "/a/b/c.txt"),
            new AlluxioURI(
                new AlluxioURI("alluxio://127.0.0.1:8080/a"), new AlluxioURI("b/c.txt"))};
    for (int i = 0; i < uriFromDifferentConstructor.length - 1; i++) {
      Assert.assertTrue(uriFromDifferentConstructor[i].equals(uriFromDifferentConstructor[i + 1]));
    }
  }

  /**
   * Tests the {@link AlluxioURI#equals(Object)} method for multi-component schemes.
   */
  @Test
  public void multiPartSchemeEqualsTest() {
    Assert.assertTrue(new AlluxioURI("scheme:part1://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("scheme:part1://127.0.0.1:3306/a.txt")));
    Assert.assertFalse(new AlluxioURI("part1://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("scheme:part1://127.0.0.1:3306/a.txt")));
    Assert.assertFalse(new AlluxioURI("scheme:part1://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("part1://127.0.0.1:3306/a.txt")));

    Assert.assertTrue(new AlluxioURI("scheme:part1:part2://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("scheme:part1:part2://127.0.0.1:3306/a.txt")));
    Assert.assertFalse(new AlluxioURI("part2://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("scheme:part1:part2://127.0.0.1:3306/a.txt")));
    Assert.assertFalse(new AlluxioURI("scheme:part1:part2://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("part2://127.0.0.1:3306/a.txt")));
  }

  /**
   * Tests the {@link AlluxioURI#equals(Object)} method with query component.
   */
  @Test
  public void queryEqualsTest() {
    Map<String, String> queryMap = new HashMap<>();
    queryMap.put("a", "b");
    queryMap.put("c", "d");

    Assert.assertTrue(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d")
        .equals(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d")));
    // There is no guarantee which order the queryMap will create the query string.
    Assert.assertTrue(new AlluxioURI("scheme://host:123/a.txt?c=d&a=b")
        .equals(new AlluxioURI("scheme", "host:123", "/a.txt", queryMap))
        || new AlluxioURI("scheme://host:123/a.txt?a=b&c=d")
        .equals(new AlluxioURI("scheme", "host:123", "/a.txt", queryMap)));

    Assert.assertFalse(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d&e=f")
        .equals(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d")));
    Assert.assertFalse(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d&e=f")
        .equals(new AlluxioURI("scheme", "host:123", "/a.txt", queryMap)));
  }

  /**
   * Tests the {@link AlluxioURI#getAuthority()} method.
   */
  @Test
  public void getAuthorityTests() {
    String[] authorities =
        new String[] {"localhost", "localhost:8080", "127.0.0.1", "127.0.0.1:8080", "localhost",
            null};
    for (String authority : authorities) {
      AlluxioURI uri = new AlluxioURI("file", authority, "/a/b");
      Assert.assertEquals(authority, uri.getAuthority());
    }

    Assert.assertEquals(null, new AlluxioURI("file", "", "/b/c").getAuthority());
    Assert.assertEquals(null, new AlluxioURI("file", null, "/b/c").getAuthority());
    Assert.assertEquals(null, new AlluxioURI("file:///b/c").getAuthority());
  }

  /**
   * Tests the {@link AlluxioURI#getDepth()} method.
   */
  @Test
  public void getDepthTests() {
    Assert.assertEquals(0, new AlluxioURI("").getDepth());
    Assert.assertEquals(0, new AlluxioURI(".").getDepth());
    Assert.assertEquals(0, new AlluxioURI("/").getDepth());
    Assert.assertEquals(1, new AlluxioURI("/a").getDepth());
    Assert.assertEquals(3, new AlluxioURI("/a/b/c.txt").getDepth());
    Assert.assertEquals(2, new AlluxioURI("/a/b/").getDepth());
    Assert.assertEquals(2, new AlluxioURI("a\\b").getDepth());
    Assert.assertEquals(1, new AlluxioURI("C:\\a").getDepth());
    Assert.assertEquals(1, new AlluxioURI("C:\\\\a").getDepth());
    Assert.assertEquals(0, new AlluxioURI("C:\\\\").getDepth());
    Assert.assertEquals(0, new AlluxioURI("alluxio://localhost:1998/").getDepth());
    Assert.assertEquals(1, new AlluxioURI("alluxio://localhost:1998/a").getDepth());
    Assert.assertEquals(2, new AlluxioURI("alluxio://localhost:1998/a/b.txt").getDepth());
  }

  /**
   * Tests the {@link AlluxioURI#getHost()} method.
   */
  @Test
  public void getHostTests() {
    Assert.assertEquals(null, new AlluxioURI("/").getHost());
    Assert.assertEquals(null, new AlluxioURI("file", "", "/a/b.txt").getHost());
    Assert.assertEquals(null, new AlluxioURI("file", null, "/a/b.txt").getHost());
    Assert.assertEquals("localhost", new AlluxioURI("s3", "localhost", "/a/b.txt").getHost());
    Assert.assertEquals("localhost", new AlluxioURI("s3", "localhost:8080", "/a/b.txt").getHost());
    Assert.assertEquals("127.0.0.1", new AlluxioURI("s3", "127.0.0.1", "/a/b.txt").getHost());
    Assert.assertEquals("127.0.0.1", new AlluxioURI("s3", "127.0.0.1:8080", "/a/b.txt").getHost());
  }

  /**
   * Tests the {@link AlluxioURI#getName()} method.
   */
  @Test
  public void getNameTests() {
    Assert.assertEquals("", new AlluxioURI("/").getName());
    Assert.assertEquals("", new AlluxioURI("alluxio://localhost/").getName());
    Assert.assertEquals("", new AlluxioURI("alluxio:/").getName());
    Assert.assertEquals("a", new AlluxioURI("alluxio:/a/").getName());
    Assert.assertEquals("a.txt", new AlluxioURI("alluxio:/a.txt/").getName());
    Assert.assertEquals(" b.txt", new AlluxioURI("alluxio:/a/ b.txt").getName());
    Assert.assertEquals("a.txt", new AlluxioURI("/a/a.txt").getName());
  }

  /**
   * Tests the {@link AlluxioURI#getParent()} method.
   */
  @Test
  public void getParentTests() {
    Assert.assertEquals(null, new AlluxioURI("/").getParent());
    Assert.assertEquals(null,
        new AlluxioURI("alluxio://localhost/").getParent());
    Assert.assertEquals(new AlluxioURI("alluxio://localhost/"),
        new AlluxioURI("alluxio://localhost/a").getParent());
    Assert.assertEquals(new AlluxioURI("/a"), new AlluxioURI("/a/b/../c").getParent());
    Assert.assertEquals(new AlluxioURI("alluxio:/a"),
        new AlluxioURI("alluxio:/a/b/../c").getParent());
    Assert.assertEquals(new AlluxioURI("alluxio://localhost:80/a"), new AlluxioURI(
        "alluxio://localhost:80/a/b/../c").getParent());
  }

  /**
   * Tests the {@link AlluxioURI#getPath()} method.
   */
  @Test
  public void getPathTests() {
    Assert.assertEquals("/", new AlluxioURI("/").getPath());
    Assert.assertEquals("/", new AlluxioURI("alluxio:/").getPath());
    Assert.assertEquals("/", new AlluxioURI("alluxio://localhost:80/").getPath());
    Assert.assertEquals("/a.txt", new AlluxioURI("alluxio://localhost:80/a.txt").getPath());
    Assert.assertEquals("/b", new AlluxioURI("alluxio://localhost:80/a/../b").getPath());
    Assert.assertEquals("/b", new AlluxioURI("alluxio://localhost:80/a/c/../../b").getPath());
    Assert.assertEquals("/a/b", new AlluxioURI("alluxio://localhost:80/a/./b").getPath());
    Assert.assertEquals("/a/b", new AlluxioURI("/a/b").getPath());
    Assert.assertEquals("/a/b", new AlluxioURI("file:///a/b").getPath());
  }

  /**
   * Tests the {@link AlluxioURI#getPort()} method.
   */
  @Test
  public void getPortTests() {
    Assert.assertEquals(-1, new AlluxioURI("/").getPort());
    Assert.assertEquals(-1, new AlluxioURI("alluxio:/").getPort());
    Assert.assertEquals(-1, new AlluxioURI("alluxio://127.0.0.1/").getPort());
    Assert.assertEquals(-1, new AlluxioURI("alluxio://localhost/").getPort());
    Assert.assertEquals(8080, new AlluxioURI("alluxio://localhost:8080/").getPort());
    Assert.assertEquals(8080, new AlluxioURI("alluxio://127.0.0.1:8080/").getPort());
  }

  /**
   * Tests the {@link AlluxioURI#getScheme()} method.
   */
  @Test
  public void getSchemeTests() {
    Assert.assertEquals(null, new AlluxioURI("/").getScheme());
    Assert.assertEquals("file", new AlluxioURI("file:/").getScheme());
    Assert.assertEquals("file", new AlluxioURI("file://localhost/").getScheme());
    Assert.assertEquals("alluxio-ft", new AlluxioURI("alluxio-ft://localhost/").getScheme());
    Assert.assertEquals("s3", new AlluxioURI("s3://localhost/").getScheme());
    Assert.assertEquals("alluxio", new AlluxioURI("alluxio://localhost/").getScheme());
    Assert.assertEquals("hdfs", new AlluxioURI("hdfs://localhost/").getScheme());
    Assert.assertEquals("glusterfs", new AlluxioURI("glusterfs://localhost/").getScheme());
    Assert.assertEquals("scheme:part1", new AlluxioURI("scheme:part1://localhost/").getScheme());
    Assert.assertEquals("scheme:part1:part2",
        new AlluxioURI("scheme:part1:part2://localhost/").getScheme());
  }

  /**
   * Tests the {@link AlluxioURI#hasAuthority()} method.
   */
  @Test
  public void hasAuthorityTests() {
    Assert.assertFalse(new AlluxioURI("/").hasAuthority());
    Assert.assertFalse(new AlluxioURI("file:/").hasAuthority());
    Assert.assertFalse(new AlluxioURI("file:///test").hasAuthority());
    Assert.assertTrue(new AlluxioURI("file://localhost/").hasAuthority());
    Assert.assertTrue(new AlluxioURI("file://localhost:8080/").hasAuthority());
    Assert.assertTrue(new AlluxioURI(null, "localhost:8080", "/").hasAuthority());
    Assert.assertTrue(new AlluxioURI(null, "localhost", "/").hasAuthority());
  }

  /**
   * Tests the {@link AlluxioURI#hasScheme()} method.
   */
  @Test
  public void hasScheme() {
    Assert.assertFalse(new AlluxioURI("/").hasScheme());
    Assert.assertTrue(new AlluxioURI("file:/").hasScheme());
    Assert.assertTrue(new AlluxioURI("file://localhost/").hasScheme());
    Assert.assertTrue(new AlluxioURI("file://localhost:8080/").hasScheme());
    Assert.assertFalse(new AlluxioURI("//localhost:8080/").hasScheme());
  }

  /**
   * Tests the {@link AlluxioURI#isAbsolute()} method.
   */
  @Test
  public void isAbsoluteTests() {
    Assert.assertTrue(new AlluxioURI("file:/a").isAbsolute());
    Assert.assertTrue(new AlluxioURI("file://localhost/a").isAbsolute());
    Assert.assertFalse(new AlluxioURI("//localhost/a").isAbsolute());
    Assert.assertFalse(new AlluxioURI("//localhost/").isAbsolute());
    Assert.assertFalse(new AlluxioURI("/").isAbsolute());
  }

  /**
   * Tests the {@link AlluxioURI#isPathAbsolute()} method.
   */
  @Test
  public void isPathAbsoluteTests() {
    Assert.assertFalse(new AlluxioURI(".").isPathAbsolute());
    Assert.assertTrue(new AlluxioURI("/").isPathAbsolute());
    Assert.assertTrue(new AlluxioURI("file:/").isPathAbsolute());
    Assert.assertTrue(new AlluxioURI("file://localhost/").isPathAbsolute());
    Assert.assertTrue(new AlluxioURI("file://localhost/a/b").isPathAbsolute());
    Assert.assertFalse(new AlluxioURI("a/b").isPathAbsolute());
    Assert.assertTrue(new AlluxioURI("C:\\\\a\\b").isPathAbsolute());
  }

  /**
   * Tests the {@link AlluxioURI#isRoot()} method.
   */
  @Test
  public void isRootTests() {
    Assert.assertFalse(new AlluxioURI(".").isRoot());
    Assert.assertTrue(new AlluxioURI("/").isRoot());
    Assert.assertTrue(new AlluxioURI("file:/").isRoot());
    Assert.assertTrue(new AlluxioURI("alluxio://localhost:19998").isRoot());
    Assert.assertTrue(new AlluxioURI("alluxio://localhost:19998/").isRoot());
    Assert.assertTrue(new AlluxioURI("hdfs://localhost:19998").isRoot());
    Assert.assertTrue(new AlluxioURI("hdfs://localhost:19998/").isRoot());
    Assert.assertTrue(new AlluxioURI("file://localhost/").isRoot());
    Assert.assertFalse(new AlluxioURI("file://localhost/a/b").isRoot());
    Assert.assertFalse(new AlluxioURI("a/b").isRoot());
  }

  /**
   * Tests the {@link AlluxioURI#join(String)} and {@link AlluxioURI#join(AlluxioURI)} methods.
   */
  @Test
  public void joinTests() {
    Assert.assertEquals(new AlluxioURI("/a"), new AlluxioURI("/").join("a"));
    Assert.assertEquals(new AlluxioURI("/a"), new AlluxioURI("/").join(new AlluxioURI("a")));
    Assert.assertEquals(new AlluxioURI("/a/b"), new AlluxioURI("/a").join(new AlluxioURI("b")));
    Assert.assertEquals(new AlluxioURI("a/b"), new AlluxioURI("a").join(new AlluxioURI("b")));
    Assert.assertEquals(new AlluxioURI("/a/c"),
        new AlluxioURI("/a").join(new AlluxioURI("b/../c")));
    Assert.assertEquals(new AlluxioURI("a/b.txt"),
        new AlluxioURI("a").join(new AlluxioURI("/b.txt")));
    Assert.assertEquals(new AlluxioURI("a/b.txt"),
        new AlluxioURI("a").join(new AlluxioURI("/c/../b.txt")));
    Assert.assertEquals(new AlluxioURI("alluxio:/a/b.txt"),
        new AlluxioURI("alluxio:/a").join("/b.txt"));
    Assert.assertEquals(new AlluxioURI("alluxio:/a/b.txt"),
        new AlluxioURI("alluxio:/a/c.txt").join(new AlluxioURI("/../b.txt")));
    Assert.assertEquals(new AlluxioURI("C:\\\\a\\b"),
        new AlluxioURI("C:\\\\a").join(new AlluxioURI("\\b")));

    final String pathWithSpecialChar = "����,��b����$o����[| =B����";
    Assert.assertEquals(new AlluxioURI("/" + pathWithSpecialChar),
            new AlluxioURI("/").join(pathWithSpecialChar));

    final String pathWithSpecialCharAndColon = "����,��b����$o����[| =B��:��";
    Assert.assertEquals(new AlluxioURI("/" + pathWithSpecialCharAndColon),
        new AlluxioURI("/").join(pathWithSpecialCharAndColon));
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor to work with file URIs
   * appropriately.
   */
  @Test
  public void fileUriTests() {
    AlluxioURI uri = new AlluxioURI("file:///foo/bar");
    Assert.assertFalse(uri.hasAuthority());
    Assert.assertEquals("/foo/bar", uri.getPath());
    Assert.assertEquals("file:///foo/bar", uri.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor to work with Windows paths
   * appropriately.
   */
  @Test
  public void windowsPathTests() {
    Assume.assumeTrue(WINDOWS);

    AlluxioURI uri = new AlluxioURI("C:\\foo\\bar");
    Assert.assertFalse(uri.hasAuthority());
    Assert.assertEquals("/C:/foo/bar", uri.getPath());
    Assert.assertEquals("C:/foo/bar", uri.toString());
  }

  /**
   * Tests the {@link AlluxioURI#toString()} method to work with paths appropriately.
   */
  @Test
  public void toStringTests() {
    String[] uris =
        new String[] {"/", "/a", "/a/ b", "alluxio://a/b/c d.txt",
            "alluxio://localhost:8080/a/b.txt", "foo", "foo/bar", "/foo/bar#boo", "foo/bar#boo",
            "file:///foo/bar"};
    for (String uri : uris) {
      AlluxioURI turi = new AlluxioURI(uri);
      Assert.assertEquals(uri, turi.toString());
    }

    Assert.assertEquals("", new AlluxioURI(".").toString());
    Assert.assertEquals("file:///a", new AlluxioURI("file:///a").toString());
    Assert.assertEquals("file:///a", new AlluxioURI("file", null, "/a").toString());
  }

  /**
   * Tests the {@link AlluxioURI#toString()} method to work with Windows paths appropriately.
   */
  @Test
  public void toStringWindowsTests() {
    Assume.assumeTrue(WINDOWS);

    String[] uris =
        new String[] { "c:/", "c:/foo/bar", "C:/foo/bar#boo", "C:/foo/ bar" };
    for (String uri : uris) {
      AlluxioURI turi = new AlluxioURI(uri);
      Assert.assertEquals(uri, turi.toString());
    }

    Assert.assertEquals("C:/", new AlluxioURI("C:\\\\").toString());
    Assert.assertEquals("C:/a/b.txt", new AlluxioURI("C:\\\\a\\b.txt").toString());
  }

  /**
   * Tests the {@link AlluxioURI#toString()} method to normalize paths.
   */
  @Test
  public void normalizeTests() {
    Assert.assertEquals("/", new AlluxioURI("//").toString());
    Assert.assertEquals("/foo", new AlluxioURI("/foo/").toString());
    Assert.assertEquals("/foo", new AlluxioURI("/foo/").toString());
    Assert.assertEquals("foo", new AlluxioURI("foo/").toString());
    Assert.assertEquals("foo", new AlluxioURI("foo//").toString());
    Assert.assertEquals("foo/bar", new AlluxioURI("foo//bar").toString());

    Assert.assertEquals("foo/boo", new AlluxioURI("foo/bar/..//boo").toString());
    Assert.assertEquals("foo/boo/baz", new AlluxioURI("foo/bar/..//boo/./baz").toString());
    Assert.assertEquals("../foo/boo", new AlluxioURI("../foo/bar/..//boo").toString());
    Assert.assertEquals("/../foo/boo", new AlluxioURI("/.././foo/boo").toString());
    Assert.assertEquals("foo/boo", new AlluxioURI("./foo/boo").toString());

    Assert.assertEquals("foo://bar boo:8080/abc/c",
        new AlluxioURI("foo://bar boo:8080/abc///c").toString());
  }

  /**
   * Tests the {@link AlluxioURI#toString()} method to normalize Windows paths.
   */
  @Test
  public void normalizeWindowsTests() {
    Assume.assumeTrue(WINDOWS);

    Assert.assertEquals("c:/a/b", new AlluxioURI("c:\\a\\b").toString());
    Assert.assertEquals("c:/a/c", new AlluxioURI("c:\\a\\b\\..\\c").toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String, String, String)} constructor to throw an
   * exception in case an empty path was provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void constructFromEmptyPathTest2() {
    new AlluxioURI(null, null, null);
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String, String, String)} constructor to throw an
   * exception in case an empty path was provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void constructFromEmptyPathTest3() {
    new AlluxioURI("file", null, "");
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor to throw an exception in case an
   * invalid URI was provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void invalidURISyntaxTest() {
    new AlluxioURI("://localhost:8080/a");
  }

  private void testParentChild(String target, String parent, String child) {
    if (target.length() > 0) {
      Assert.assertEquals(new AlluxioURI(target), new AlluxioURI(new AlluxioURI(parent),
          new AlluxioURI(child)));
    } else {
      Assert.assertEquals(target,
          new AlluxioURI(new AlluxioURI(parent), new AlluxioURI(child)).toString());
    }
  }

  /**
   * Tests the {@link AlluxioURI#getLeadingPath(int)} method.
   */
  @Test
  public void getLeadingPathTest() {
    Assert.assertEquals("/",      new AlluxioURI("/a/b/c/").getLeadingPath(0));
    Assert.assertEquals("/a",     new AlluxioURI("/a/b/c/").getLeadingPath(1));
    Assert.assertEquals("/a/b",   new AlluxioURI("/a/b/c/").getLeadingPath(2));
    Assert.assertEquals("/a/b/c", new AlluxioURI("/a/b/c/").getLeadingPath(3));
    Assert.assertEquals(null,     new AlluxioURI("/a/b/c/").getLeadingPath(4));

    Assert.assertEquals("/",      new AlluxioURI("/").getLeadingPath(0));

    Assert.assertEquals("",       new AlluxioURI("").getLeadingPath(0));
    Assert.assertEquals(null,     new AlluxioURI("").getLeadingPath(1));
    Assert.assertEquals("",       new AlluxioURI(".").getLeadingPath(0));
    Assert.assertEquals(null,     new AlluxioURI(".").getLeadingPath(1));
    Assert.assertEquals("a/b",    new AlluxioURI("a/b/c").getLeadingPath(1));
  }
}
