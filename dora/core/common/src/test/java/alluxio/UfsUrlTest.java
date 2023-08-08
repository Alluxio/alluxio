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

import alluxio.uri.SingleMasterAuthority;
import alluxio.uri.UfsUrl;

import org.junit.Test;

/**
 * Unit tests for {@link UfsUrl}.
 */
public class UfsUrlTest {

  /**
   * Tests the {@link UfsUrl#createInstance(String)} constructor for basic Alluxio paths.
   */
  @Test
  public void basicUfsUrl() {
    // deprecate it.
    UfsUrl ufsUrl = UfsUrl.createInstance("abc://localhost:19998/xy z/a b c");

    assertTrue(ufsUrl.getAuthority().isPresent());
    assertEquals("localhost:19998", ufsUrl.getAuthority().get().toString());
    SingleMasterAuthority authority = (SingleMasterAuthority) ufsUrl.getAuthority().get();
    assertEquals("localhost", authority.getHost());
    assertEquals(19998, authority.getPort());

    assertEquals(2, ufsUrl.getDepth());
    assertEquals("a b c", ufsUrl.getName());
    assertTrue(ufsUrl.getScheme().isPresent());
    assertEquals("abc", ufsUrl.getScheme().get());
    assertEquals("abc://localhost:19998/xy z", ufsUrl.getParentURL().asString());
    assertEquals("abc://localhost:19998/", ufsUrl.getParentURL().getParentURL().asString());
    assertEquals("/xy z/a b c", ufsUrl.getFullPath());
    assertEquals("abc://localhost:19998/xy z/a b c/d", ufsUrl.join("/d").asString());
    // TODO(Tony Sun): Rewrite join function of UfsUrl, add join test in the future.
    assertEquals("abc://localhost:19998/xy z/a b c", ufsUrl.asString());
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
    assertEquals("hdfs://localhost:8020/xy z", ufsUrl.getParentURL().asString());
    assertEquals("hdfs://localhost:8020/", ufsUrl.getParentURL().getParentURL().asString());
    assertEquals("/xy z/a b c", ufsUrl.getFullPath());
    assertTrue(ufsUrl.getScheme().isPresent());
    assertEquals("hdfs", ufsUrl.getScheme().get());
    assertEquals("hdfs://localhost:8020/xy z/a b c/d", ufsUrl.join("/d").asString());
    // TODO(Tony Sun): Rewrite join function of UfsUrl, add join test in the future.
    assertEquals("hdfs://localhost:8020/xy z/a b c", ufsUrl.asString());
  }
}
