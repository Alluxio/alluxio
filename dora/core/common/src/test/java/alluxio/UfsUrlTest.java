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
//    assertEquals("a b c", ufsUrl.getName());
//    assertEquals("alluxio://localhost:19998/xy z", ufsUrl.getParentURL().asString());
//    assertEquals("alluxio://localhost:19998/", ufsUrl.getParentURL().getParentURL().asString());
  }
}
