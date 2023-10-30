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

/**
 * Unit tests for {@link alluxio.uri.UfsUrl}.
 */

package alluxio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.grpc.UfsUrlMessage;
import alluxio.uri.SingleMasterAuthority;
import alluxio.uri.UfsUrl;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link UfsUrl}.
 */
@Ignore
public class UfsUrlTest {

  /**
   * Tests the {@link UfsUrl#createInstance(String)} constructor for basic Alluxio paths.
   */
  @Test
  public void basicUfsUrl() {
    UfsUrl ufsUrl = UfsUrl.createInstance("abc://localhost:19998/xy z/a b c");

    assertTrue(ufsUrl.getAuthority().isPresent());
    assertEquals("localhost:19998", ufsUrl.getAuthority().get().toString());
    SingleMasterAuthority authority = (SingleMasterAuthority) ufsUrl.getAuthority().get();
    assertEquals("localhost", authority.getHost());
    assertEquals(19998, authority.getPort());

    assertEquals(2, ufsUrl.getDepth());
    assertEquals("a b c", ufsUrl.getName());
    assertEquals("abc", ufsUrl.getScheme());
    assertEquals("abc://localhost:19998/xy z", ufsUrl.getParentURL().toString());
    assertEquals("abc://localhost:19998/", ufsUrl.getParentURL().getParentURL().toString());
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
    assertEquals(authority1, ufsUrl1.getAuthority().get().toString());
    assertEquals(path1, ufsUrl1.getFullPath());

    String scheme2 = "file";
    String authority2 = "";
    String path2 = "/testDir/testFile";
    UfsUrl ufsUrl2 = new UfsUrl(scheme2, authority2, path2);
    assertEquals(scheme2, ufsUrl2.getScheme());
    assertEquals(authority2, ufsUrl2.getAuthority().get().toString());
    assertEquals(path2, ufsUrl2.getFullPath());
    assertEquals(scheme2 + "://" + authority2 + path2, ufsUrl2.toString());
  }

  /**
   * Tests the {@link UfsUrl#createInstance(String)} constructor for basic HDFS paths.
   */
  @Test
  public void basicHdfsUri() {
    UfsUrl ufsUrl = UfsUrl.createInstance("hdfs://localhost:8020/xy z/a b c");

    assertTrue(ufsUrl.getAuthority().isPresent());
    assertEquals("localhost:8020", ufsUrl.getAuthority().get().toString());
    assertTrue(ufsUrl.getAuthority().get() instanceof SingleMasterAuthority);
    SingleMasterAuthority authority = (SingleMasterAuthority) ufsUrl.getAuthority().get();
    assertEquals("localhost", authority.getHost());
    assertEquals(8020, authority.getPort());

    assertEquals(2, ufsUrl.getDepth());
    assertEquals("a b c", ufsUrl.getName());
    assertEquals("hdfs://localhost:8020/xy z", ufsUrl.getParentURL().toString());
    assertEquals("hdfs://localhost:8020/", ufsUrl.getParentURL().getParentURL().toString());
    assertEquals("/xy z/a b c", ufsUrl.getFullPath());
    assertEquals("hdfs", ufsUrl.getScheme());
    assertEquals("hdfs://localhost:8020/xy z/a b c/d", ufsUrl.join("/d").toString());
    assertEquals("hdfs://localhost:8020/xy z/a b c", ufsUrl.toString());
  }

  @Test
  public void hashCodeTest() {
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

  /**
   * Tests the {@link UfsUrl#getDepth()} method.
   */
  @Test
  public void getDepthTests() {
    assertEquals(0, UfsUrl.createInstance("abc://localhost:19998/").getDepth());
    assertEquals(1, UfsUrl.createInstance("abc://localhost:19998/a").getDepth());
    assertEquals(2, UfsUrl.createInstance("abc://localhost:19998/a/b.txt").getDepth());
  }
}
