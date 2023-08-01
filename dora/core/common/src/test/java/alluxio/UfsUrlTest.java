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

public class UfsUrlTest {

  @Test
  public void basicUfsUrl() {
    UfsUrl ufsUrl = new UfsUrl("alluxio://localhost:19998/xy z/a b c");
    assertTrue(ufsUrl.hasScheme());
    assertTrue(ufsUrl.hasAuthority());
    assertEquals("localhost:19998", ufsUrl.getAuthority().toString());

    SingleMasterAuthority authority = (SingleMasterAuthority) ufsUrl.getAuthority();
    assertEquals("localhost", authority.getHost());
    assertEquals(19998, authority.getPort());

//    assertEquals(2, ufsUrl.getDepth());
    assertEquals("a b c", ufsUrl.getName());
    assertTrue(ufsUrl.isAbsolute());
    assertEquals("alluxio", ufsUrl.getScheme());
    /*
    The test below is not supported, for absolute path promise.
    assertEquals("alluxio://localhost:19998/xy z", ufsUrl.getParentURL().asString());
    assertEquals("alluxio://localhost:19998/", ufsUrl.getParentURL().getParentURL().asString());
    assertEquals("/xy z/a b c", ufsUrl.getFullPath());
    assertEquals("alluxio://localhost:19998/xy z/a b c/d", ufsUrl.join("/d").asString());
    assertEquals("alluxio://localhost:19998/xy z/a b c/d", ufsUrl.join(new AlluxioURI("/d"))
        .toString());
    assertEquals("alluxio://localhost:19998/xy z/a b c", ufsUrl.asString());
*/
  }

  @Test
  public void basicTests() {
    String[] strs =
        new String[] {"alluxio://localhost:19998/xyz/abc", "hdfs://localhost:19998/xyz/abc",
            "s3://localhost:19998/xyz/abc", "alluxio://localhost:19998/xy z/a b c",
            "hdfs://localhost:19998/xy z/a b c", "s3://localhost:19998/xy z/a b c"};
    for (String str : strs) {
      UfsUrl uri = new UfsUrl(str);
//      assertEquals(str, uri.asString());
//      assertEquals(2, uri.getDepth());
      assertTrue(uri.getAuthority() instanceof SingleMasterAuthority);
      SingleMasterAuthority authority = (SingleMasterAuthority) uri.getAuthority();
      assertEquals("localhost", authority.getHost());
      assertEquals(19998, authority.getPort());
    }
  }

}
