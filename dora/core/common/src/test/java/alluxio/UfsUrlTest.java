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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.exception.InvalidPathException;
import alluxio.grpc.UfsUrlMessage;
import alluxio.uri.EmbeddedLogicalAuthority;
import alluxio.uri.NoAuthority;
import alluxio.uri.SingleMasterAuthority;
import alluxio.uri.UfsUrl;
import alluxio.uri.UnknownAuthority;
import alluxio.uri.ZookeeperAuthority;
import alluxio.uri.ZookeeperLogicalAuthority;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link UfsUrl}.
 */
public class UfsUrlTest {

  /**
   * Tests the {@link UfsUrl#createInstance(String)} constructor for basic Alluxio paths.
   */

  @Test
  public void basicUfsUrl() {
    UfsUrl ufsUrl = UfsUrl.createInstance("abc://localhost:19998/xy z/a b c");

    assertNotNull(ufsUrl.getAuthority());
    assertEquals("localhost:19998", ufsUrl.getAuthority().toString());
    SingleMasterAuthority authority = (SingleMasterAuthority) ufsUrl.getAuthority();
    assertEquals("localhost", authority.getHost());
    assertEquals(19998, authority.getPort());

    assertEquals(2, ufsUrl.getDepth());
    assertEquals("a b c", ufsUrl.getName());
    assertEquals("abc", ufsUrl.getScheme());
    assertEquals("abc://localhost:19998/xy z", ufsUrl.getParentURL().get().toString());
    assertEquals("abc://localhost:19998/",
        ufsUrl.getParentURL().get().getParentURL().get().toString());
    assertEquals("/xy z/a b c", ufsUrl.getFullPath());
    assertEquals("abc://localhost:19998/xy z/a b c/d", ufsUrl.join("/d").toString());
    assertEquals("abc://localhost:19998/xy z/a b c", ufsUrl.toString());
  }

  @Test
  public void UfsUrlCtor() {
    String scheme1 = "abc";
    String authority1 = "127.0.0.1:4567";
    String path1 = "/";
    UfsUrl ufsUrl1 = new UfsUrl(scheme1, authority1, path1);
    assertEquals(scheme1, ufsUrl1.getScheme());
    assertEquals(authority1, ufsUrl1.getAuthority().toString());
    assertEquals(path1, ufsUrl1.getFullPath());

    String scheme2 = "file";
    String authority2 = "";
    String path2 = "/testDir/testFile";
    UfsUrl ufsUrl2 = new UfsUrl(scheme2, authority2, path2);
    assertEquals(scheme2, ufsUrl2.getScheme());
    assertEquals(authority2, ufsUrl2.getAuthority().toString());
    assertEquals(path2, ufsUrl2.getFullPath());
    assertEquals(scheme2 + "://" + authority2 + path2, ufsUrl2.toString());

    String ufsUrlString3 = "xyz:testFolder";
    UfsUrl ufsUrl3 = UfsUrl.createInstance(ufsUrlString3);
    assertEquals("xyz", ufsUrl3.getScheme());

    String ufsUrlString4 = "xyz://localhost:9999";
    UfsUrl ufsUrl4 = UfsUrl.createInstance(ufsUrlString4);
    assertEquals("localhost:9999", ufsUrl4.getAuthority().toString());
  }

  /**
   * Tests the {@link UfsUrl#createInstance(String)} constructor for basic HDFS paths.
   */
  @Test
  public void basicHdfsUri() {
    UfsUrl ufsUrl = UfsUrl.createInstance("hdfs://localhost:8020/xy z/a b c");

    assertNotNull(ufsUrl.getAuthority());
    assertEquals("localhost:8020", ufsUrl.getAuthority().toString());
    assertTrue(ufsUrl.getAuthority() instanceof SingleMasterAuthority);
    SingleMasterAuthority authority = (SingleMasterAuthority) ufsUrl.getAuthority();
    assertEquals("localhost", authority.getHost());
    assertEquals(8020, authority.getPort());

    assertEquals(2, ufsUrl.getDepth());
    assertEquals("a b c", ufsUrl.getName());
    assertEquals("hdfs://localhost:8020/xy z", ufsUrl.getParentURL().get().toString());
    assertEquals("hdfs://localhost:8020/",
        ufsUrl.getParentURL().get().getParentURL().get().toString());
    assertEquals("/xy z/a b c", ufsUrl.getFullPath());
    assertEquals("hdfs", ufsUrl.getScheme());
    assertEquals("hdfs://localhost:8020/xy z/a b c/d", ufsUrl.join("/d").toString());
    assertEquals("hdfs://localhost:8020/xy z/a b c", ufsUrl.toString());
  }

  @Test
  public void hashCodeTestOfUfsUrl() {
    String scheme = "xyz";
    String authority = "192.168.0.1:9527";
    String path = "a/b/c/d";
    String ufsUrlPath = scheme + "://" + authority + "/" + path;
    List<String> pathComponents = Arrays.asList(path.split(UfsUrl.SLASH_SEPARATOR));

    UfsUrl ufsUrl1 = UfsUrl.createInstance(ufsUrlPath);
    UfsUrl ufsUrl2 = UfsUrl.createInstance(UfsUrlMessage.newBuilder()
        .setScheme(scheme).setAuthority(authority).addAllPathComponents(pathComponents).build());
    assertEquals(ufsUrl1.toString(), ufsUrl2.toString());
    assertEquals(ufsUrl1.hashCode(), ufsUrl2.hashCode());

    UfsUrl ufsUrl3 = new UfsUrl(scheme, authority, path);
    assertEquals(ufsUrl2.toString(), ufsUrl3.toString());
    assertEquals(ufsUrl2.hashCode(), ufsUrl3.hashCode());
  }

  @Test
  public void pathWithWhiteSpacesNoException() throws IllegalArgumentException {
    String[] paths = new String[]{
        "file:/// ",
        "file:///  ",
        "file:/// path",
        "file:///path ",
        "file:///pa th",
        "file:/// pa th ",
        "file:///pa/ th",
        "file:///pa / th",
        "file:// pa / th "
    };
    for (String path : paths) {
      UfsUrl ufsUrl = UfsUrl.createInstance(path);
      assertEquals(path, ufsUrl.toString());
    }
  }

  /**
   * Tests the {@link UfsUrl#createInstance(String)} constructor for URL
   * without scheme or with invalid scheme.
   */
  @Test
  public void pathWithoutSchemeException() {
    String path1 =  "/ ";
    Exception e1 = assertThrows(IllegalArgumentException.class, () -> {
      UfsUrl ufsUrl = UfsUrl.createInstance(path1);
    });
    assertTrue(e1.getMessage().contains(String.format("empty scheme: %s", path1)));

    String path2 = "file:  / / /path";
    Exception e2 = assertThrows(IllegalArgumentException.class, () -> {
      UfsUrl ufsUrl = UfsUrl.createInstance(path2);
    });
    assertTrue(e2.getMessage().contains(String.format("empty scheme: %s", path2)));

    String path3 =  "abc/:";
    Exception e3 = assertThrows(IllegalArgumentException.class, () -> {
      UfsUrl ufsUrl = UfsUrl.createInstance(path3);
    });
    assertTrue(e3.getMessage().contains(String.format("empty scheme: %s", path3)));
  }

  /**
   * Tests the {@link UfsUrl#UfsUrl(String, String, String)} constructor to build a URI
   * from its different components.
   */
  @Test
  public void constructFromComponents() {
    String scheme = "xyz";
    String authority = "127.0.0.1:90909";
    String path = "/a/b/c.txt";

    UfsUrl uri1 = new UfsUrl(scheme, "", path);
    assertEquals(scheme + "://" + path, uri1.toString());

    UfsUrl uri2 = new UfsUrl(scheme, authority, path);
    assertEquals(scheme + "://" + authority + path, uri2.toString());
  }

  /**
   * Tests the {@link UfsUrl#equals(Object)} method.
   */
  @Test
  public void equals() {
    assertNotEquals(UfsUrl.createInstance("xyz://127.0.0.1:8080/a/b/c.txt"),
        UfsUrl.createInstance("xyz://localhost:8080/a/b/c.txt"));

    UfsUrl ufsUrl1 = UfsUrl.createInstance("xyz://127.0.0.1:8080/a/b/c.txt");

    assertNotEquals(ufsUrl1, ufsUrl1.toProto());
    assertEquals(ufsUrl1, ufsUrl1);

    UfsUrl ufsUrl2 = UfsUrl.createInstance("xyz://127.0.0.1:8080/a/b/c.txt");
    UfsUrl ufsUrl3 = new UfsUrl("xyz", "127.0.0.1:8080", "/a/b/c.txt");
    assertEquals(ufsUrl1, ufsUrl2);
    assertEquals(ufsUrl2, ufsUrl3);
  }

  /**
   * Tests the {@link UfsUrl#getAuthority()} method.
   */
  @Test
  public void getAuthority() {
    String[] authorities =
        new String[] {"localhost", "localhost:8080", "127.0.0.1", "127.0.0.1:8080", "localhost"};
    for (String authority : authorities) {
      UfsUrl uri = new UfsUrl("file", authority, "/a/b");
      assertNotNull(uri.getAuthority());
      assertEquals(authority, uri.getAuthority().toString());
    }

    assertEquals("",
        new UfsUrl("file", "", "/b/c").getAuthority().toString());
    assertEquals("", UfsUrl.createInstance("file:///b/c").getAuthority().toString());
  }

  @Test
  public void authorityTypes() {
    assertTrue(new UfsUrl("file", "localhost:8080", "/b/c")
        .getAuthority() instanceof SingleMasterAuthority);

    assertTrue(new UfsUrl("file", "zk@host:2181", "/b/c")
        .getAuthority() instanceof ZookeeperAuthority);
    assertTrue(UfsUrl.createInstance("xxx://zk@host1:2181,host2:2181,host3:2181/b/c").getAuthority()
        instanceof ZookeeperAuthority);
    assertTrue(UfsUrl.createInstance("xxx://zk@host1:2181;host2:2181;host3:2181/b/c").getAuthority()
        instanceof ZookeeperAuthority);

    assertTrue(new UfsUrl("file", "", "/b/c").getAuthority()
        instanceof NoAuthority);
    assertTrue(UfsUrl.createInstance("file:///b/c").getAuthority() instanceof NoAuthority);

    assertTrue(new UfsUrl("file", "ebj@logical", "/b/c").getAuthority()
        instanceof EmbeddedLogicalAuthority);

    assertTrue(new UfsUrl("file", "zk@logical", "/b/c").getAuthority()
        instanceof ZookeeperLogicalAuthority);

    assertTrue(new UfsUrl("file", "localhost", "/b/c").getAuthority()
        instanceof UnknownAuthority);
  }

  /**
   * Tests the {@link UfsUrl#getDepth()} method.
   */
  @Test
  public void getDepth() {
    assertEquals(0, UfsUrl.createInstance("abc://localhost:19998/").getDepth());
    assertEquals(1, UfsUrl.createInstance("abc://localhost:19998/a").getDepth());
    assertEquals(2, UfsUrl.createInstance("abc://localhost:19998/a/b.txt").getDepth());
  }

  /**
   * Tests the {@link UfsUrl#toProto()} method.
   */
  @Test
  public void toProto() {
    String scheme = "abc";
    String authority = "localhost:6666";
    String path = "testFolder/testFile";
    List<String> pathList = Arrays.asList(path.split(UfsUrl.SLASH_SEPARATOR));
    UfsUrlMessage ufsUrlMessage = UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authority)
        .addAllPathComponents(pathList)
        .build();
    UfsUrl ufsUrl = new UfsUrl(scheme, authority, path);
    assertEquals(ufsUrlMessage, ufsUrl.toProto());
  }

  /**
   * Tests the {@link UfsUrl#fromProto(UfsUrlMessage)} method.
   */
  @Test
  public void fromProto() {
    String scheme = "abc";
    String authority = "localhost:6666";
    String path = "testFolder/testFile";
    List<String> pathList = Arrays.asList(path.split(UfsUrl.SLASH_SEPARATOR));
    UfsUrlMessage ufsUrlMessage = UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authority)
        .addAllPathComponents(pathList)
        .build();
    UfsUrl ufsUrl = new UfsUrl(scheme, authority, path);
    assertEquals(ufsUrl, UfsUrl.fromProto(ufsUrlMessage));
  }

  /**
   * Tests the {@link UfsUrl#toAlluxioURI()} method.
   */
  @Test
  public void toAlluxioURI() {
    String url = "abc://6.7.8.9:9988/testFolder1/testFolder2/a a/b cde/#$%^&*83fhb";
    UfsUrl ufsUrl = UfsUrl.createInstance(url);
    AlluxioURI uri = new AlluxioURI(url);
    assertEquals(uri, ufsUrl.toAlluxioURI());
  }

  /**
   * Tests the {@link UfsUrl#getName()} method.
   */
  @Test
  public void getName() {
    assertEquals("", UfsUrl.createInstance("abc://").getName());
    assertEquals("a", UfsUrl.createInstance("abc:/a/").getName());
    assertEquals("a.txt", UfsUrl.createInstance("abc:/a.txt/").getName());
    assertEquals(" b.txt", UfsUrl.createInstance("abc:/a/ b.txt").getName());
    assertEquals("", UfsUrl.createInstance("abc://localhost/").getName());
  }

  /**
   * Tests the {@link UfsUrl#getParentURL()} method.
   */
  @Test
  public void getParent() {
    assertFalse(UfsUrl.createInstance("abc://localhost/").getParentURL().isPresent());
    assertEquals(UfsUrl.createInstance("abc://localhost/"),
        UfsUrl.createInstance("abc://localhost/a").getParentURL().get());
    assertEquals(UfsUrl.createInstance("abc:/a/b"),
        UfsUrl.createInstance("abc:/a/b/c").getParentURL().get());
    assertEquals(UfsUrl.createInstance("abc:/a"),
        UfsUrl.createInstance("abc:/a/c").getParentURL().get());
    assertEquals(UfsUrl.createInstance("abc://localhost:80/a"),
        UfsUrl.createInstance("abc://localhost:80/a/b").getParentURL().get());

    assertFalse(UfsUrl.createInstance("abc:").getParentURL().isPresent());
    assertFalse(UfsUrl.createInstance("abc:/").getParentURL().isPresent());
    assertFalse(UfsUrl.createInstance("abc://").getParentURL().isPresent());
    assertFalse(UfsUrl.createInstance("abc:///").getParentURL().isPresent());
  }

  /**
   * Tests the {@link UfsUrl#getFullPath()} method.
   */
  @Test
  public void getPath() {
    assertEquals("/", UfsUrl.createInstance("abc:/").getFullPath());
    assertEquals("/", UfsUrl.createInstance("abc://localhost:80/").getFullPath());
    assertEquals("/a.txt", UfsUrl.createInstance("abc://localhost:80/a.txt").getFullPath());
    assertEquals("/b", UfsUrl.createInstance("abc://localhost:80/b").getFullPath());
    assertEquals("/b", UfsUrl.createInstance("abc://localhost:80/b").getFullPath());
    assertEquals("/a/b", UfsUrl.createInstance("abc://localhost:80/a/b").getFullPath());
    assertEquals("/a/b", UfsUrl.createInstance("abc://localhost:80/a/b/").getFullPath());
  }

  /**
   * Tests the {@link UfsUrl#getScheme()} method.
   */
  @Test
  public void getScheme() {
    assertEquals("file", UfsUrl.createInstance("file:/").getScheme());
    assertEquals("file", UfsUrl.createInstance("file://localhost/").getScheme());
    assertEquals("s3", UfsUrl.createInstance("s3://localhost/").getScheme());
    assertEquals("qwer", UfsUrl.createInstance("qwer://localhost/").getScheme());
    assertEquals("hdfs", UfsUrl.createInstance("hdfs://localhost/").getScheme());
    assertEquals("glusterfs", UfsUrl.createInstance("glusterfs://localhost/").getScheme());
  }

  @Test
  public void isAncestorOf() throws InvalidPathException {
    UfsUrl ufsUrl1 = UfsUrl.createInstance("abc://1.2.3.4/testFolder1/testFolder2");
    UfsUrl ufsUrl2 = UfsUrl.createInstance("abc://1.2.3.5/testFolder1");
    UfsUrl ufsUrl3 = UfsUrl.createInstance("xyz://1.2.3.5/testFolder1/");
    UfsUrl ufsUrl4 = UfsUrl.createInstance("abc://1.2.3.5/test");
    UfsUrl ufsUrl5 = UfsUrl.createInstance("abc://1.2.3.4/testFolder1/");
    UfsUrl ufsUrl6 = UfsUrl.createInstance("abc://1.2.3.5/testFolder1/testFolder2");

    assertFalse(ufsUrl3.isAncestorOf(ufsUrl1));
    assertFalse(ufsUrl2.isAncestorOf(ufsUrl1));
    assertFalse(ufsUrl4.isAncestorOf(ufsUrl2));
    assertFalse(ufsUrl3.isAncestorOf(ufsUrl6));
    assertTrue(ufsUrl5.isAncestorOf(ufsUrl1));
  }

  /**
   * Tests the {@link AlluxioURI#join(String)} and {@link AlluxioURI#join(AlluxioURI)} methods.
   */
  @Test
  public void join() {
    assertEquals(UfsUrl.createInstance("abc:/a"), UfsUrl.createInstance("abc:/").join("a"));
    assertEquals(UfsUrl.createInstance("abc:/a/b.txt"),
        UfsUrl.createInstance("abc:/a").join("/b.txt"));

    final String pathWithSpecialChar = "根目录";
    assertEquals(UfsUrl.createInstance("abc:/" + pathWithSpecialChar),
        UfsUrl.createInstance("abc:/").join(pathWithSpecialChar));

    final String pathWithSpecialCharAndColon = "根目录";
    assertEquals(UfsUrl.createInstance("abc:/" + pathWithSpecialCharAndColon),
        UfsUrl.createInstance("abc:/").join(pathWithSpecialCharAndColon));

    // join empty string
    assertEquals(UfsUrl.createInstance("abc:/a"), UfsUrl.createInstance("abc:/a").join(""));
  }

  /**
   * Tests the {@link UfsUrl#createInstance(String)} constructor to work with file URIs
   * appropriately.
   */
  @Test
  public void fileUrl() {
    UfsUrl url = UfsUrl.createInstance("file:///foo/bar");
    assertTrue(url.getAuthority().toString().isEmpty());
    assertEquals("/foo/bar", url.getFullPath());
    assertEquals("file:///foo/bar", url.toString());
  }

  /**
   * Tests the {@link UfsUrl#toString()} method to normalize paths.
   */
  @Test
  public void normalize() {
    assertEquals("xyz:///", UfsUrl.createInstance("xyz://").toString());
    assertEquals("xyz:///foo", UfsUrl.createInstance("xyz:/foo/").toString());
    assertEquals("xyz:///foo", UfsUrl.createInstance("xyz:/foo/").toString());
    assertEquals("xyz:///foo", UfsUrl.createInstance("xyz:/foo/").toString());
    assertEquals("xyz:///foo", UfsUrl.createInstance("xyz:/foo//").toString());
    assertEquals("xyz:///foo/bar", UfsUrl.createInstance("xyz:/foo//bar").toString());

    assertEquals("xyz:///foo/bar/boo", UfsUrl.createInstance("xyz:/foo/bar/boo").toString());
    assertEquals("xyz:///foo/boo/baz",
        UfsUrl.createInstance("xyz:/foo/////boo/baz").toString());
    // TODO(Yichuan Sun): Is this case needed?
//    assertEquals("xyz:///foo/boo", UfsUrl.createInstance("xyz:./foo/boo").toString());

    assertEquals("foo://bar boo:8080/abc/c",
        UfsUrl.createInstance("foo://bar boo:8080/abc///c").toString());
  }

  /**
   * Tests the {@link UfsUrl#UfsUrl(String, String, String)} constructor to throw an
   * exception in case an empty path was provided.
   */
  @Test(expected = NullPointerException.class)
  public void constructFromEmptyPathTest2() {
    new UfsUrl(null, null, null);
  }

  /**
   * Tests the {@link UfsUrl#UfsUrl(String, String, String)} constructor to throw an
   * exception in case an empty path was provided.
   */
  @Test(expected = NullPointerException.class)
  public void constructFromEmptyPathTest3() {
    new UfsUrl("file", null, "");
  }

  /**
   * Tests the {@link UfsUrl#createInstance(String)} constructor to throw an exception in case an
   * invalid URI was provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void invalidURISyntax() {
    UfsUrl.createInstance("://localhost:8080/a");
  }
}
