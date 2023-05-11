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

package alluxio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import alluxio.uri.Authority;
import alluxio.uri.EmbeddedLogicalAuthority;
import alluxio.uri.MultiMasterAuthority;
import alluxio.uri.NoAuthority;
import alluxio.uri.SingleMasterAuthority;
import alluxio.uri.UnknownAuthority;
import alluxio.uri.ZookeeperAuthority;
import alluxio.uri.ZookeeperLogicalAuthority;
import alluxio.util.OSUtils;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link AlluxioURI}.
 */
public class AlluxioURITest {

  private static final boolean WINDOWS = OSUtils.isWindows();

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor for basic Alluxio paths.
   */
  @Test
  public void basicAlluxioUri() {
    AlluxioURI uri = new AlluxioURI("alluxio://localhost:19998/xy z/a b c");

    assertTrue(uri.hasAuthority());
    assertEquals("localhost:19998", uri.getAuthority().toString());
    assertTrue(uri.getAuthority() instanceof SingleMasterAuthority);
    SingleMasterAuthority authority = (SingleMasterAuthority) uri.getAuthority();
    assertEquals("localhost", authority.getHost());
    assertEquals(19998, authority.getPort());

    assertEquals(2, uri.getDepth());
    assertEquals("a b c", uri.getName());
    assertEquals("alluxio://localhost:19998/xy z", uri.getParent().toString());
    assertEquals("alluxio://localhost:19998/", uri.getParent().getParent().toString());
    assertEquals("/xy z/a b c", uri.getPath());
    assertEquals("alluxio", uri.getScheme());
    assertTrue(uri.hasScheme());
    assertTrue(uri.isAbsolute());
    assertTrue(uri.isPathAbsolute());
    assertEquals("alluxio://localhost:19998/xy z/a b c/d", uri.join("/d").toString());
    assertEquals("alluxio://localhost:19998/xy z/a b c/d", uri.join(new AlluxioURI("/d"))
        .toString());
    assertEquals("alluxio://localhost:19998/xy z/a b c", uri.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor for basic HDFS paths.
   */
  @Test
  public void basicHdfsUri() {
    AlluxioURI uri = new AlluxioURI("hdfs://localhost:8020/xy z/a b c");

    assertTrue(uri.hasAuthority());
    assertEquals("localhost:8020", uri.getAuthority().toString());
    assertTrue(uri.getAuthority() instanceof SingleMasterAuthority);
    SingleMasterAuthority authority = (SingleMasterAuthority) uri.getAuthority();
    assertEquals("localhost", authority.getHost());
    assertEquals(8020, authority.getPort());

    assertEquals(2, uri.getDepth());
    assertEquals("a b c", uri.getName());
    assertEquals("hdfs://localhost:8020/xy z", uri.getParent().toString());
    assertEquals("hdfs://localhost:8020/", uri.getParent().getParent().toString());
    assertEquals("/xy z/a b c", uri.getPath());
    assertEquals("hdfs", uri.getScheme());
    assertTrue(uri.hasScheme());
    assertTrue(uri.isAbsolute());
    assertTrue(uri.isPathAbsolute());
    assertEquals("hdfs://localhost:8020/xy z/a b c/d", uri.join("/d").toString());
    assertEquals("hdfs://localhost:8020/xy z/a b c/d", uri.join(new AlluxioURI("/d"))
        .toString());
    assertEquals("hdfs://localhost:8020/xy z/a b c", uri.toString());
  }

  @Test
  public void basicTwoPartUri() {
    AlluxioURI uri = new AlluxioURI("scheme:part2://localhost:8000/xy z/a b c");
    assertEquals(uri, new AlluxioURI("scheme:part2//localhost:8000/xy z/a b c"));
    assertEquals("scheme:part2", uri.getScheme());

    assertTrue(uri.hasAuthority());
    assertEquals("localhost:8000", uri.getAuthority().toString());
    assertTrue(uri.getAuthority() instanceof SingleMasterAuthority);
    SingleMasterAuthority authority = (SingleMasterAuthority) uri.getAuthority();
    assertEquals("localhost", authority.getHost());
    assertEquals(8000, authority.getPort());

    assertEquals(2, uri.getDepth());
    assertEquals("a b c", uri.getName());
    assertEquals("scheme:part2://localhost:8000/xy z", uri.getParent().toString());
    assertEquals("scheme:part2://localhost:8000/", uri.getParent().getParent().toString());
    assertEquals("/xy z/a b c", uri.getPath());
    assertTrue(uri.hasScheme());
    assertTrue(uri.isAbsolute());
    assertTrue(uri.isPathAbsolute());
    assertEquals("scheme:part2://localhost:8000/xy z/a b c/d", uri.join("/d").toString());
    assertEquals("scheme:part2://localhost:8000/xy z/a b c/d", uri.join(new AlluxioURI("/d"))
        .toString());
    assertEquals("scheme:part2://localhost:8000/xy z/a b c", uri.toString());
  }

  @Test
  public void basicMultiMasterUri() {
    AlluxioURI uri = new AlluxioURI("alluxio://host1:19998,host2:19998,host3:19998/xy z/a b c");

    assertTrue(uri.hasAuthority());
    assertEquals("host1:19998,host2:19998,host3:19998", uri.getAuthority().toString());
    assertTrue(uri.getAuthority() instanceof MultiMasterAuthority);

    assertEquals(2, uri.getDepth());
    assertEquals("a b c", uri.getName());
    assertEquals("alluxio://host1:19998,host2:19998,host3:19998/xy z", uri.getParent().toString());
    assertEquals("alluxio://host1:19998,host2:19998,host3:19998/",
        uri.getParent().getParent().toString());
    assertEquals("/xy z/a b c", uri.getPath());
    assertEquals("alluxio", uri.getScheme());
    assertTrue(uri.hasScheme());
    assertTrue(uri.isAbsolute());
    assertTrue(uri.isPathAbsolute());
    assertEquals("alluxio://host1:19998,host2:19998,host3:19998/xy z/a b c/d",
        uri.join("/d").toString());
    assertEquals("alluxio://host1:19998,host2:19998,host3:19998/xy z/a b c/d",
        uri.join(new AlluxioURI("/d")).toString());
    assertEquals("alluxio://host1:19998,host2:19998,host3:19998/xy z/a b c", uri.toString());
  }

  @Test
  public void semicolonMultiMasterUri() {
    AlluxioURI uri =
        new AlluxioURI("alluxio://host1:1323;host2:54325;host3:64354/xy z/a b c");
    assertTrue(uri.hasAuthority());
    assertEquals("host1:1323,host2:54325,host3:64354", uri.getAuthority().toString());
    assertTrue(uri.getAuthority() instanceof MultiMasterAuthority);
    MultiMasterAuthority authority = (MultiMasterAuthority) uri.getAuthority();
    assertEquals("host1:1323,host2:54325,host3:64354", authority.getMasterAddresses());
  }

  @Test
  public void plusMultiMasterUri() {
    AlluxioURI uri =
        new AlluxioURI("alluxio://host1:526+host2:54325+host3:624/xy z/a b c");
    assertTrue(uri.hasAuthority());
    assertTrue(uri.getAuthority() instanceof MultiMasterAuthority);
    MultiMasterAuthority authority = (MultiMasterAuthority) uri.getAuthority();
    assertEquals("host1:526,host2:54325,host3:624", authority.getMasterAddresses());
  }

  @Test
  public void basicZookeeperUri() {
    AlluxioURI uri =
        new AlluxioURI("alluxio://zk@host1:2181,host2:2181,host3:2181/xy z/a b c");
    assertEquals(uri,
        new AlluxioURI("alluxio://zk@host1:2181,host2:2181,host3:2181/xy z/a b c"));
    assertEquals("alluxio", uri.getScheme());

    assertEquals("zk@host1:2181,host2:2181,host3:2181", uri.getAuthority().toString());
    assertTrue(uri.getAuthority() instanceof ZookeeperAuthority);
    ZookeeperAuthority zkAuthority = (ZookeeperAuthority) uri.getAuthority();
    assertEquals("host1:2181,host2:2181,host3:2181", zkAuthority.getZookeeperAddress());

    assertEquals(2, uri.getDepth());
    assertEquals("a b c", uri.getName());
    assertEquals("alluxio://zk@host1:2181,host2:2181,host3:2181/xy z",
        uri.getParent().toString());
    assertEquals("alluxio://zk@host1:2181,host2:2181,host3:2181/",
        uri.getParent().getParent().toString());
    assertEquals("/xy z/a b c", uri.getPath());
    assertTrue(uri.hasAuthority());
    assertTrue(uri.hasScheme());
    assertTrue(uri.isAbsolute());
    assertTrue(uri.isPathAbsolute());
    assertEquals("alluxio://zk@host1:2181,host2:2181,host3:2181/xy z/a b c/d",
        uri.join("/d").toString());
    assertEquals("alluxio://zk@host1:2181,host2:2181,host3:2181/xy z/a b c/d",
        uri.join(new AlluxioURI("/d"))
        .toString());
    assertEquals("alluxio://zk@host1:2181,host2:2181,host3:2181/xy z/a b c",
        uri.toString());
  }

  @Test
  public void semicolonZookeeperUri() {
    AlluxioURI uri =
        new AlluxioURI("alluxio://zk@host1:2181;host2:2181;host3:2181/xy z/a b c");
    assertTrue(uri.hasAuthority());
    assertEquals("zk@host1:2181,host2:2181,host3:2181", uri.getAuthority().toString());
    assertTrue(uri.getAuthority() instanceof ZookeeperAuthority);
    ZookeeperAuthority zkAuthority = (ZookeeperAuthority) uri.getAuthority();
    assertEquals("host1:2181,host2:2181,host3:2181", zkAuthority.getZookeeperAddress());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor with query parameter.
   */
  @Test
  public void basicConstructorQuery() {
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
    assertEquals(4, queryMap.size());
    assertEquals("v1", queryMap.get("k1"));
    assertEquals(" spaces ", queryMap.get("k2"));
    assertEquals("= escapes  %&+", queryMap.get("k3"));
    assertEquals("[]{};\"'<>,./", queryMap.get("!@#$^*()-_"));
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
      assertEquals(str, uri.toString());
      assertEquals(2, uri.getDepth());
      assertTrue(uri.getAuthority() instanceof SingleMasterAuthority);
      SingleMasterAuthority authority = (SingleMasterAuthority) uri.getAuthority();
      assertEquals("localhost", authority.getHost());
      assertEquals(19998, authority.getPort());
    }
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor for an empty URI.
   */
  @Test
  public void emptyURI() {
    AlluxioURI uri = new AlluxioURI("");
    assertEquals("", uri.getAuthority().toString());
    assertTrue(uri.getAuthority() instanceof NoAuthority);
    assertEquals(0, uri.getDepth());
    assertEquals("", uri.getName());
    assertEquals("", uri.getPath());
    assertEquals(null, uri.getScheme());
    assertFalse(uri.hasAuthority());
    assertFalse(uri.hasScheme());
    assertFalse(uri.isAbsolute());
    assertFalse(uri.isPathAbsolute());
    assertEquals("/d", uri.join("/d").toString());
    assertEquals("/d", uri.join(new AlluxioURI("/d")).toString());
    assertEquals("", uri.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor for URI with spaces.
   */
  @Test
  public void pathWithWhiteSpaces() {
    String[] paths = new String[]{
        "/ ",
        "/  ",
        "/ path",
        "/path ",
        "/pa th",
        "/ pa th ",
        "/pa/ th",
        "/pa / th",
        "/ pa / th ",
    };
    for (String path : paths) {
      AlluxioURI uri = new AlluxioURI(path);
      assertEquals(path, uri.getPath());
      assertEquals(path, uri.toString());
      assertTrue(uri.isPathAbsolute());
    }
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String, Authority, String)} constructor to build a URI
   * from its different components.
   */
  @Test
  public void constructFromComponentsTests() {
    String scheme = "alluxio";
    String authority = "127.0.0.1:90909";
    String path = "/a/../b/c.txt";
    String absPath = "/b/c.txt";

    AlluxioURI uri0 = new AlluxioURI(null, null, path);
    assertEquals(absPath, uri0.toString());

    AlluxioURI uri1 = new AlluxioURI(scheme, null, path);
    assertEquals(scheme + "://" + absPath, uri1.toString());

    AlluxioURI uri2 = new AlluxioURI(scheme, Authority.fromString(authority), path);
    assertEquals(scheme + "://" + authority + absPath, uri2.toString());

    AlluxioURI uri3 = new AlluxioURI(null, Authority.fromString(authority), path);
    assertEquals("//" + authority + absPath, uri3.toString());

    AlluxioURI uri4 = new AlluxioURI("scheme:part1", Authority.fromString(authority), path);
    assertEquals("scheme:part1://" + authority + absPath, uri4.toString());

    AlluxioURI uri5 = new AlluxioURI("scheme:part1:part2", Authority.fromString(authority), path);
    assertEquals("scheme:part1:part2://" + authority + absPath, uri5.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String, Authority, String, Map)} constructor to build an
   * URI from its different components with a query map.
   */
  @Test
  public void constructWithQueryMap() {
    String scheme = "alluxio";
    String authority = "host:1234";
    String path = "/a";
    Map<String, String> queryMap = new HashMap<>();
    queryMap.put("key", "123");
    queryMap.put(" k2 ", " v2 ");
    queryMap.put(" key: !*'();:@&=+$,/?#[]\"% ", " !*'();:@&=+$,/?#[]\"% ");
    queryMap.put(" key: %26 %3D %20 %25 %2B ", " %26 %3D %20 %25 %2B ");

    AlluxioURI uri1 = new AlluxioURI(scheme, Authority.fromString(authority), path, queryMap);
    AlluxioURI uri2 = new AlluxioURI(uri1.toString());
    assertEquals(queryMap, uri1.getQueryMap());
    assertEquals(uri1.getQueryMap(), uri2.getQueryMap());
  }

  /**
   * Tests to resolve a child {@link AlluxioURI} against a parent {@link AlluxioURI}.
   */
  @Test
  public void constructFromParentAndChildTests() {
    testParentChild("", ".", ".");
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
      assertTrue(uris[i].compareTo(uris[i + 1]) < 0);
      assertTrue(uris[i + 1].compareTo(uris[i]) > 0);
      assertEquals(0, uris[i].compareTo(uris[i]));
    }
  }

  /**
   * Tests the {@link AlluxioURI#equals(Object)} method.
   */
  @Test
  public void equalsTests() {
    assertFalse(new AlluxioURI("alluxio://127.0.0.1:8080/a/b/c.txt").equals(new AlluxioURI(
        "alluxio://localhost:8080/a/b/c.txt")));

    AlluxioURI[] uriFromDifferentConstructor =
        new AlluxioURI[] {new AlluxioURI("alluxio://127.0.0.1:8080/a/b/c.txt"),
            new AlluxioURI("alluxio", Authority.fromString("127.0.0.1:8080"), "/a/b/c.txt"),
            new AlluxioURI(
                new AlluxioURI("alluxio://127.0.0.1:8080/a"), new AlluxioURI("b/c.txt"))};
    for (int i = 0; i < uriFromDifferentConstructor.length - 1; i++) {
      assertTrue(uriFromDifferentConstructor[i].equals(uriFromDifferentConstructor[i + 1]));
    }
  }

  /**
   * Tests the {@link AlluxioURI#equals(Object)} method for multi-component schemes.
   */
  @Test
  public void multiPartSchemeEquals() {
    assertTrue(new AlluxioURI("scheme:part1://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("scheme:part1://127.0.0.1:3306/a.txt")));
    assertFalse(new AlluxioURI("part1://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("scheme:part1://127.0.0.1:3306/a.txt")));
    assertFalse(new AlluxioURI("scheme:part1://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("part1://127.0.0.1:3306/a.txt")));

    assertTrue(new AlluxioURI("scheme:part1:part2://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("scheme:part1:part2://127.0.0.1:3306/a.txt")));
    assertFalse(new AlluxioURI("part2://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("scheme:part1:part2://127.0.0.1:3306/a.txt")));
    assertFalse(new AlluxioURI("scheme:part1:part2://127.0.0.1:3306/a.txt")
        .equals(new AlluxioURI("part2://127.0.0.1:3306/a.txt")));

    new EqualsTester()
        .addEqualityGroup(new AlluxioURI("sch:p1:p2://aaaabbbb:12345/"),
            new AlluxioURI("sch:p1:p2://aaaabbbb:12345/"))
        .addEqualityGroup(new AlluxioURI("standard://host:12345/"))
        .testEquals();
  }

  /**
   * Tests the {@link AlluxioURI#equals(Object)} method with query component.
   */
  @Test
  public void queryEquals() {
    Map<String, String> queryMap = new HashMap<>();
    queryMap.put("a", "b");
    queryMap.put("c", "d");

    assertTrue(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d")
        .equals(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d")));
    // There is no guarantee which order the queryMap will create the query string.
    assertTrue(new AlluxioURI("scheme://host:123/a.txt?c=d&a=b")
        .equals(new AlluxioURI("scheme", Authority.fromString("host:123"), "/a.txt", queryMap))
        || new AlluxioURI("scheme://host:123/a.txt?a=b&c=d")
        .equals(new AlluxioURI("scheme", Authority.fromString("host:123"), "/a.txt", queryMap)));

    assertFalse(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d&e=f")
        .equals(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d")));
    assertFalse(new AlluxioURI("scheme://host:123/a.txt?a=b&c=d&e=f")
        .equals(new AlluxioURI("scheme", Authority.fromString("host:123"), "/a.txt", queryMap)));
  }

  /**
   * Tests the {@link AlluxioURI#getAuthority()} method.
   */
  @Test
  public void getAuthorityTests() {
    String[] authorities =
        new String[] {"localhost", "localhost:8080", "127.0.0.1", "127.0.0.1:8080", "localhost"};
    for (String authority : authorities) {
      AlluxioURI uri = new AlluxioURI("file", Authority.fromString(authority), "/a/b");
      assertEquals(authority, uri.getAuthority().toString());
    }

    assertEquals("",
        new AlluxioURI("file", Authority.fromString(""), "/b/c").getAuthority().toString());
    assertEquals("", new AlluxioURI("file", null, "/b/c").getAuthority().toString());
    assertEquals("",
        new AlluxioURI("file", Authority.fromString(null), "/b/c").getAuthority().toString());
    assertEquals("", new AlluxioURI("file:///b/c").getAuthority().toString());
  }

  @Test
  public void authorityTypeTests() {
    assertTrue(new AlluxioURI("file", Authority.fromString("localhost:8080"), "/b/c").getAuthority()
        instanceof SingleMasterAuthority);

    assertTrue(new AlluxioURI("file", Authority.fromString("zk@host:2181"), "/b/c").getAuthority()
        instanceof ZookeeperAuthority);
    assertTrue(new AlluxioURI("alluxio://zk@host1:2181,host2:2181,host3:2181/b/c").getAuthority()
        instanceof ZookeeperAuthority);
    assertTrue(new AlluxioURI("alluxio://zk@host1:2181;host2:2181;host3:2181/b/c").getAuthority()
        instanceof ZookeeperAuthority);

    assertTrue(new AlluxioURI("file", Authority.fromString(""), "/b/c").getAuthority()
        instanceof NoAuthority);
    assertTrue(new AlluxioURI("file", null, "/b/c").getAuthority()
        instanceof NoAuthority);
    assertTrue(new AlluxioURI("file", Authority.fromString(null), "/b/c").getAuthority()
        instanceof NoAuthority);
    assertTrue(new AlluxioURI("file:///b/c").getAuthority()
        instanceof NoAuthority);

    assertTrue(new AlluxioURI("file", Authority.fromString("ebj@logical"), "/b/c").getAuthority()
        instanceof EmbeddedLogicalAuthority);

    assertTrue(new AlluxioURI("file", Authority.fromString("zk@logical"), "/b/c").getAuthority()
        instanceof ZookeeperLogicalAuthority);

    assertTrue(new AlluxioURI("file", Authority.fromString("localhost"), "/b/c").getAuthority()
        instanceof UnknownAuthority);
  }

  /**
   * Tests the {@link AlluxioURI#getDepth()} method.
   */
  @Test
  public void getDepthTests() {
    assertEquals(0, new AlluxioURI("").getDepth());
    assertEquals(0, new AlluxioURI(".").getDepth());
    assertEquals(0, new AlluxioURI("/").getDepth());
    assertEquals(1, new AlluxioURI("/a").getDepth());
    assertEquals(3, new AlluxioURI("/a/b/c.txt").getDepth());
    assertEquals(2, new AlluxioURI("/a/b/").getDepth());
    assertEquals(2, new AlluxioURI("a\\b").getDepth());
    assertEquals(1, new AlluxioURI("C:\\a").getDepth());
    assertEquals(1, new AlluxioURI("C:\\\\a").getDepth());
    assertEquals(0, new AlluxioURI("C:\\\\").getDepth());
    assertEquals(0, new AlluxioURI("alluxio://localhost:19998/").getDepth());
    assertEquals(1, new AlluxioURI("alluxio://localhost:19998/a").getDepth());
    assertEquals(2, new AlluxioURI("alluxio://localhost:19998/a/b.txt").getDepth());
  }

  /**
   * Tests the {@link AlluxioURI#getName()} method.
   */
  @Test
  public void getNameTests() {
    assertEquals(".", new AlluxioURI(".").getName());
    assertEquals("", new AlluxioURI("/").getName());
    assertEquals("", new AlluxioURI("alluxio://localhost/").getName());
    assertEquals("", new AlluxioURI("alluxio:/").getName());
    assertEquals("a", new AlluxioURI("alluxio:/a/").getName());
    assertEquals("a.txt", new AlluxioURI("alluxio:/a.txt/").getName());
    assertEquals(" b.txt", new AlluxioURI("alluxio:/a/ b.txt").getName());
    assertEquals("a.txt", new AlluxioURI("/a/a.txt").getName());
  }

  /**
   * Tests the {@link AlluxioURI#getParent()} method.
   */
  @Test
  public void getParentTests() {
    assertEquals(null, new AlluxioURI("/").getParent());
    assertEquals(null,
        new AlluxioURI("alluxio://localhost/").getParent());
    assertEquals(new AlluxioURI("alluxio://localhost/"),
        new AlluxioURI("alluxio://localhost/a").getParent());
    assertEquals(new AlluxioURI("/a"), new AlluxioURI("/a/b/../c").getParent());
    assertEquals(new AlluxioURI("alluxio:/a"),
        new AlluxioURI("alluxio:/a/b/../c").getParent());
    assertEquals(new AlluxioURI("alluxio://localhost:80/a"), new AlluxioURI(
        "alluxio://localhost:80/a/b/../c").getParent());
  }

  /**
   * Tests the {@link AlluxioURI#getPath()} method.
   */
  @Test
  public void getPathTests() {
    assertEquals(".", new AlluxioURI(".").getPath());
    assertEquals("/", new AlluxioURI("/").getPath());
    assertEquals("/", new AlluxioURI("alluxio:/").getPath());
    assertEquals("/", new AlluxioURI("alluxio://localhost:80/").getPath());
    assertEquals("/a.txt", new AlluxioURI("alluxio://localhost:80/a.txt").getPath());
    assertEquals("/b", new AlluxioURI("alluxio://localhost:80/a/../b").getPath());
    assertEquals("/b", new AlluxioURI("alluxio://localhost:80/a/c/../../b").getPath());
    assertEquals("/a/b", new AlluxioURI("alluxio://localhost:80/a/./b").getPath());
    assertEquals("/a/b", new AlluxioURI("/a/b").getPath());
    assertEquals("/a/b", new AlluxioURI("file:///a/b").getPath());
  }

  /**
   * Tests the {@link AlluxioURI#getScheme()} method.
   */
  @Test
  public void getSchemeTests() {
    assertEquals(null, new AlluxioURI(".").getScheme());
    assertEquals(null, new AlluxioURI("/").getScheme());
    assertEquals("file", new AlluxioURI("file:/").getScheme());
    assertEquals("file", new AlluxioURI("file://localhost/").getScheme());
    assertEquals("s3", new AlluxioURI("s3://localhost/").getScheme());
    assertEquals("alluxio", new AlluxioURI("alluxio://localhost/").getScheme());
    assertEquals("hdfs", new AlluxioURI("hdfs://localhost/").getScheme());
    assertEquals("glusterfs", new AlluxioURI("glusterfs://localhost/").getScheme());
    assertEquals("scheme:part1", new AlluxioURI("scheme:part1://localhost/").getScheme());
    assertEquals("scheme:part1:part2",
        new AlluxioURI("scheme:part1:part2://localhost/").getScheme());
  }

  /**
   * Tests the {@link AlluxioURI#hasAuthority()} method.
   */
  @Test
  public void hasAuthorityTests() {
    assertFalse(new AlluxioURI(".").hasAuthority());
    assertFalse(new AlluxioURI("/").hasAuthority());
    assertFalse(new AlluxioURI("file:/").hasAuthority());
    assertFalse(new AlluxioURI("file:///test").hasAuthority());
    assertTrue(new AlluxioURI("file://localhost/").hasAuthority());
    assertTrue(new AlluxioURI("file://localhost:8080/").hasAuthority());
    assertTrue(new AlluxioURI(null, Authority.fromString("localhost:8080"), "/").hasAuthority());
    assertTrue(new AlluxioURI(null, Authority.fromString("localhost"), "/").hasAuthority());
  }

  /**
   * Tests the {@link AlluxioURI#hasScheme()} method.
   */
  @Test
  public void hasScheme() {
    assertFalse(new AlluxioURI("/").hasScheme());
    assertTrue(new AlluxioURI("file:/").hasScheme());
    assertTrue(new AlluxioURI("file://localhost/").hasScheme());
    assertTrue(new AlluxioURI("file://localhost:8080/").hasScheme());
    assertFalse(new AlluxioURI("//localhost:8080/").hasScheme());
  }

  /**
   * Tests the {@link AlluxioURI#isAbsolute()} method.
   */
  @Test
  public void isAbsoluteTests() {
    assertTrue(new AlluxioURI("file:/a").isAbsolute());
    assertTrue(new AlluxioURI("file://localhost/a").isAbsolute());
    assertFalse(new AlluxioURI("//localhost/a").isAbsolute());
    assertFalse(new AlluxioURI("//localhost/").isAbsolute());
    assertFalse(new AlluxioURI("/").isAbsolute());
  }

  /**
   * Tests the {@link AlluxioURI#isPathAbsolute()} method.
   */
  @Test
  public void isPathAbsoluteTests() {
    assertFalse(new AlluxioURI(".").isPathAbsolute());
    assertTrue(new AlluxioURI("/").isPathAbsolute());
    assertTrue(new AlluxioURI("file:/").isPathAbsolute());
    assertTrue(new AlluxioURI("file://localhost/").isPathAbsolute());
    assertTrue(new AlluxioURI("file://localhost/a/b").isPathAbsolute());
    assertFalse(new AlluxioURI("a/b").isPathAbsolute());
    assertTrue(new AlluxioURI("C:\\\\a\\b").isPathAbsolute());
  }

  /**
   * Tests the {@link AlluxioURI#isRoot()} method.
   */
  @Test
  public void isRootTests() {
    assertFalse(new AlluxioURI(".").isRoot());
    assertTrue(new AlluxioURI("/").isRoot());
    assertTrue(new AlluxioURI("file:/").isRoot());
    assertTrue(new AlluxioURI("alluxio://localhost:19998").isRoot());
    assertTrue(new AlluxioURI("alluxio://localhost:19998/").isRoot());
    assertTrue(new AlluxioURI("hdfs://localhost:19998").isRoot());
    assertTrue(new AlluxioURI("hdfs://localhost:19998/").isRoot());
    assertTrue(new AlluxioURI("file://localhost/").isRoot());
    assertFalse(new AlluxioURI("file://localhost/a/b").isRoot());
    assertFalse(new AlluxioURI("a/b").isRoot());
  }

  /**
   * Tests the {@link AlluxioURI#join(String)} and {@link AlluxioURI#join(AlluxioURI)} methods.
   */
  @Test
  public void joinTests() {
    assertEquals(new AlluxioURI("/a"), new AlluxioURI("/").join("a"));
    assertEquals(new AlluxioURI("/a"), new AlluxioURI("/").join(new AlluxioURI("a")));
    assertEquals(new AlluxioURI("/a/b"), new AlluxioURI("/a").join(new AlluxioURI("b")));
    assertEquals(new AlluxioURI("a/b"), new AlluxioURI("a").join(new AlluxioURI("b")));
    assertEquals(new AlluxioURI("/a/c"),
        new AlluxioURI("/a").join(new AlluxioURI("b/../c")));
    assertEquals(new AlluxioURI("a/b.txt"),
        new AlluxioURI("a").join(new AlluxioURI("/b.txt")));
    assertEquals(new AlluxioURI("a/b.txt"),
        new AlluxioURI("a").join(new AlluxioURI("/c/../b.txt")));
    assertEquals(new AlluxioURI("alluxio:/a/b.txt"),
        new AlluxioURI("alluxio:/a").join("/b.txt"));
    assertEquals(new AlluxioURI("alluxio:/a/b.txt"),
        new AlluxioURI("alluxio:/a/c.txt").join(new AlluxioURI("/../b.txt")));
    assertEquals(new AlluxioURI("C:\\\\a\\b"),
        new AlluxioURI("C:\\\\a").join(new AlluxioURI("\\b")));
    assertEquals(new AlluxioURI("/a/b"), new AlluxioURI("/a").joinUnsafe("///b///"));

    final String pathWithSpecialChar = "����,��b����$o����[| =B����";
    assertEquals(new AlluxioURI("/" + pathWithSpecialChar),
            new AlluxioURI("/").join(pathWithSpecialChar));

    final String pathWithSpecialCharAndColon = "����,��b����$o����[| =B��:��";
    assertEquals(new AlluxioURI("/" + pathWithSpecialCharAndColon),
        new AlluxioURI("/").join(pathWithSpecialCharAndColon));

    // join empty string
    assertEquals(new AlluxioURI("/a"), new AlluxioURI("/a").join(""));
    assertEquals(new AlluxioURI("/a"), new AlluxioURI("/a").join(new AlluxioURI("")));
  }

  @Test
  public void joinUnsafe() {
    assertEquals(new AlluxioURI("/a"), new AlluxioURI("/").joinUnsafe("a"));
    assertEquals(new AlluxioURI("/a/b"), new AlluxioURI("/a").joinUnsafe("b"));
    assertEquals(new AlluxioURI("a/b"), new AlluxioURI("a").joinUnsafe("b"));
    assertEquals(new AlluxioURI("a/b.txt"), new AlluxioURI("a").joinUnsafe("/b.txt"));
    assertEquals(new AlluxioURI("alluxio:/a/b.txt"),
        new AlluxioURI("alluxio:/a").joinUnsafe("/b.txt"));
    assertEquals(new AlluxioURI("C:\\\\a\\b"), new AlluxioURI("C:\\\\a").joinUnsafe("\\b"));
    assertEquals(new AlluxioURI("/a/b"), new AlluxioURI("/a").joinUnsafe("///b///"));

    final String pathWithSpecialChar = "����,��b����$o����[| =B����";
    assertEquals(new AlluxioURI("/" + pathWithSpecialChar),
        new AlluxioURI("/").joinUnsafe(pathWithSpecialChar));

    final String pathWithSpecialCharAndColon = "����,��b����$o����[| =B��:��";
    assertEquals(new AlluxioURI("/" + pathWithSpecialCharAndColon),
        new AlluxioURI("/").joinUnsafe(pathWithSpecialCharAndColon));

    // The following joins are not "safe", because the new path component requires normalization.
    assertNotEquals(new AlluxioURI("/a/c"), new AlluxioURI("/a").joinUnsafe("b/../c"));
    assertNotEquals(new AlluxioURI("a/b.txt"), new AlluxioURI("a").joinUnsafe("/c/../b.txt"));
    assertNotEquals(new AlluxioURI("alluxio:/a/b.txt"),
        new AlluxioURI("alluxio:/a/c.txt").joinUnsafe("/../b.txt"));

    // join empty string
    assertEquals(new AlluxioURI("/a"), new AlluxioURI("/a").joinUnsafe(""));
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor to work with file URIs
   * appropriately.
   */
  @Test
  public void fileUriTests() {
    AlluxioURI uri = new AlluxioURI("file:///foo/bar");
    assertFalse(uri.hasAuthority());
    assertEquals("/foo/bar", uri.getPath());
    assertEquals("file:///foo/bar", uri.toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String)} constructor to work with Windows paths
   * appropriately.
   */
  @Test
  public void windowsPathTests() {
    assumeTrue(WINDOWS);

    AlluxioURI uri = new AlluxioURI("C:\\foo\\bar");
    assertFalse(uri.hasAuthority());
    assertEquals("/C:/foo/bar", uri.getPath());
    assertEquals("C:/foo/bar", uri.toString());
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
      assertEquals(uri, turi.toString());
    }

    assertEquals(".", new AlluxioURI(".").toString());
    assertEquals("file:///a", new AlluxioURI("file:///a").toString());
    assertEquals("file:///a", new AlluxioURI("file", null, "/a").toString());
  }

  /**
   * Tests the {@link AlluxioURI#toString()} method to work with Windows paths appropriately.
   */
  @Test
  public void toStringWindowsTests() {
    assumeTrue(WINDOWS);

    String[] uris =
        new String[] { "c:/", "c:/foo/bar", "C:/foo/bar#boo", "C:/foo/ bar" };
    for (String uri : uris) {
      AlluxioURI turi = new AlluxioURI(uri);
      assertEquals(uri, turi.toString());
    }

    assertEquals("C:/", new AlluxioURI("C:\\\\").toString());
    assertEquals("C:/a/b.txt", new AlluxioURI("C:\\\\a\\b.txt").toString());
  }

  /**
   * Tests the {@link AlluxioURI#toString()} method to normalize paths.
   */
  @Test
  public void normalizeTests() {
    assertEquals("/", new AlluxioURI("//").toString());
    assertEquals("/foo", new AlluxioURI("/foo/").toString());
    assertEquals("/foo", new AlluxioURI("/foo/").toString());
    assertEquals("foo", new AlluxioURI("foo/").toString());
    assertEquals("foo", new AlluxioURI("foo//").toString());
    assertEquals("foo/bar", new AlluxioURI("foo//bar").toString());

    assertEquals("foo/boo", new AlluxioURI("foo/bar/..//boo").toString());
    assertEquals("foo/boo/baz", new AlluxioURI("foo/bar/..//boo/./baz").toString());
    assertEquals("../foo/boo", new AlluxioURI("../foo/bar/..//boo").toString());
    assertEquals("/../foo/boo", new AlluxioURI("/.././foo/boo").toString());
    assertEquals("foo/boo", new AlluxioURI("./foo/boo").toString());

    assertEquals("foo://bar boo:8080/abc/c",
        new AlluxioURI("foo://bar boo:8080/abc///c").toString());
  }

  /**
   * Tests the {@link AlluxioURI#toString()} method to normalize Windows paths.
   */
  @Test
  public void normalizeWindowsTests() {
    assumeTrue(WINDOWS);

    assertEquals("c:/a/b", new AlluxioURI("c:\\a\\b").toString());
    assertEquals("c:/a/c", new AlluxioURI("c:\\a\\b\\..\\c").toString());
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String, Authority, String)} constructor to throw an
   * exception in case an empty path was provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void constructFromEmptyPathTest2() {
    new AlluxioURI(null, null, null);
  }

  /**
   * Tests the {@link AlluxioURI#AlluxioURI(String, Authority, String)} constructor to throw an
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
  public void invalidURISyntax() {
    new AlluxioURI("://localhost:8080/a");
  }

  /**
   * Tests to resolve a child {@link AlluxioURI} against a parent {@link AlluxioURI}.
   *
   * @param target the target path
   * @param parent the parent path
   * @param child the child path
   */
  private void testParentChild(String target, String parent, String child) {
    if (target.length() > 0) {
      assertEquals(new AlluxioURI(target), new AlluxioURI(new AlluxioURI(parent),
          new AlluxioURI(child)));
    } else {
      assertEquals(target,
          new AlluxioURI(new AlluxioURI(parent), new AlluxioURI(child)).toString());
    }
  }

  /**
   * Tests the {@link AlluxioURI#getLeadingPath(int)} method.
   */
  @Test
  public void getLeadingPath() {
    assertEquals("/",      new AlluxioURI("/a/b/c/").getLeadingPath(0));
    assertEquals("/a",     new AlluxioURI("/a/b/c/").getLeadingPath(1));
    assertEquals("/a/b",   new AlluxioURI("/a/b/c/").getLeadingPath(2));
    assertEquals("/a/b/c", new AlluxioURI("/a/b/c/").getLeadingPath(3));
    assertEquals(null,     new AlluxioURI("/a/b/c/").getLeadingPath(4));

    assertEquals("/",      new AlluxioURI("/").getLeadingPath(0));

    assertEquals("",       new AlluxioURI("").getLeadingPath(0));
    assertEquals(null,     new AlluxioURI("").getLeadingPath(1));
    assertEquals(".",       new AlluxioURI(".").getLeadingPath(0));
    assertEquals(null,     new AlluxioURI(".").getLeadingPath(1));
    assertEquals("a/b",    new AlluxioURI("a/b/c").getLeadingPath(1));
  }

  /**
   * Tests the {@link AlluxioURI#getRootPath()} method.
   */
  @Test
  public void getRootPath() {
    assertEquals("s3a://s3-bucket-name/",
        new AlluxioURI("s3a://s3-bucket-name/").getRootPath());
    assertEquals("s3a://s3-bucket-name/",
        new AlluxioURI("s3a://s3-bucket-name/folder").getRootPath());
    assertEquals("/",
        new AlluxioURI("/tmp/folder").getRootPath());
  }
}
