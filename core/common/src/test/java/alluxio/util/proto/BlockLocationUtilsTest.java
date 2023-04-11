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

package alluxio.util.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import alluxio.proto.meta.Block.BlockLocation;

import org.junit.Test;

public class BlockLocationUtilsTest {
  @Test
  public void testBlockLocationCached() {
    BlockLocation location1 = BlockLocationUtils.getCached(1, "HDD", "SSD");
    assertEquals("HDD", location1.getTier());
    assertEquals("SSD", location1.getMediumType());
    assertEquals(1, location1.getWorkerId());

    BlockLocation location2 = BlockLocationUtils.getCached(1, "HDD", "SSD");
    assertSame(location1, location2);
    assertEquals(location1, location2);

    BlockLocation location3 = BlockLocationUtils.getCached(location2);
    assertSame(location1, location3);
    assertEquals(location1, location3);

    BlockLocationUtils.evictByWorkerId(1);

    BlockLocation location4 = BlockLocationUtils.getCached(1, "HDD", "SSD");
    assertNotSame(location1, location4);
    assertEquals(location1, location4);
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidValue() {
    BlockLocationUtils.getCached(1, "INVALID", "SSD");
  }
}
