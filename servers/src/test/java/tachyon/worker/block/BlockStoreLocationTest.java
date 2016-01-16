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

package tachyon.worker.block;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link BlockStoreLocation}.
 */
public class BlockStoreLocationTest {

  /**
   * Tests that a new location can be created with the constructor.
   */
  @Test
  public void newLocationTest() {
    String tierAlias = "SSD";
    int dirIndex = 3;
    BlockStoreLocation loc = new BlockStoreLocation(tierAlias, dirIndex);
    Assert.assertNotNull(loc);
    Assert.assertEquals(tierAlias, loc.tierAlias());
    Assert.assertEquals(dirIndex, loc.dir());
  }

  /**
   * Tests the {@link BlockStoreLocation#belongTo(BlockStoreLocation)} method.
   */
  @Test
  public void testBelongTo() {
    BlockStoreLocation anyTier = BlockStoreLocation.anyTier();
    BlockStoreLocation anyDirInTierMEM =
        BlockStoreLocation.anyDirInTier("MEM");
    BlockStoreLocation anyDirInTierHDD =
        BlockStoreLocation.anyDirInTier("HDD");
    BlockStoreLocation dirInMEM = new BlockStoreLocation("MEM", 1);
    BlockStoreLocation dirInHDD = new BlockStoreLocation("HDD", 2);

    Assert.assertTrue(anyTier.belongTo(anyTier));
    Assert.assertFalse(anyTier.belongTo(anyDirInTierMEM));
    Assert.assertFalse(anyTier.belongTo(anyDirInTierHDD));
    Assert.assertFalse(anyTier.belongTo(dirInMEM));
    Assert.assertFalse(anyTier.belongTo(dirInHDD));

    Assert.assertTrue(anyDirInTierMEM.belongTo(anyTier));
    Assert.assertTrue(anyDirInTierMEM.belongTo(anyDirInTierMEM));
    Assert.assertFalse(anyDirInTierMEM.belongTo(anyDirInTierHDD));
    Assert.assertFalse(anyDirInTierMEM.belongTo(dirInMEM));
    Assert.assertFalse(anyDirInTierMEM.belongTo(dirInHDD));

    Assert.assertTrue(anyDirInTierHDD.belongTo(anyTier));
    Assert.assertFalse(anyDirInTierHDD.belongTo(anyDirInTierMEM));
    Assert.assertTrue(anyDirInTierHDD.belongTo(anyDirInTierHDD));
    Assert.assertFalse(anyDirInTierHDD.belongTo(dirInMEM));
    Assert.assertFalse(anyDirInTierHDD.belongTo(dirInHDD));

    Assert.assertTrue(dirInMEM.belongTo(anyTier));
    Assert.assertTrue(dirInMEM.belongTo(anyDirInTierMEM));
    Assert.assertFalse(dirInMEM.belongTo(anyDirInTierHDD));
    Assert.assertTrue(dirInMEM.belongTo(dirInMEM));
    Assert.assertFalse(dirInMEM.belongTo(dirInHDD));

    Assert.assertTrue(dirInHDD.belongTo(anyTier));
    Assert.assertFalse(dirInHDD.belongTo(anyDirInTierMEM));
    Assert.assertTrue(dirInHDD.belongTo(anyDirInTierHDD));
    Assert.assertFalse(dirInHDD.belongTo(dirInMEM));
    Assert.assertTrue(dirInHDD.belongTo(dirInHDD));
  }

  /**
   * Tests the {@link BlockStoreLocation#equals(Object)} method.
   */
  @Test
  public void equalsTest() {
    BlockStoreLocation anyTier = BlockStoreLocation.anyTier();
    BlockStoreLocation anyDirInTierMEM =
        BlockStoreLocation.anyDirInTier("MEM");
    BlockStoreLocation anyDirInTierHDD =
        BlockStoreLocation.anyDirInTier("HDD");
    BlockStoreLocation dirInMEM = new BlockStoreLocation("MEM", 1);
    BlockStoreLocation dirInHDD = new BlockStoreLocation("HDD", 2);

    Assert.assertEquals(anyTier, BlockStoreLocation.anyTier()); // Equals
    Assert.assertNotEquals(anyDirInTierMEM, BlockStoreLocation.anyTier());
    Assert.assertNotEquals(anyDirInTierHDD, BlockStoreLocation.anyTier());
    Assert.assertNotEquals(dirInMEM, BlockStoreLocation.anyTier());
    Assert.assertNotEquals(dirInHDD, BlockStoreLocation.anyTier());

    Assert.assertNotEquals(anyTier, BlockStoreLocation.anyDirInTier("MEM"));
    Assert.assertEquals(anyDirInTierMEM, BlockStoreLocation.anyDirInTier("MEM")); // Equals
    Assert.assertNotEquals(anyDirInTierHDD, BlockStoreLocation.anyDirInTier("MEM"));
    Assert.assertNotEquals(dirInMEM, BlockStoreLocation.anyDirInTier("MEM"));
    Assert.assertNotEquals(dirInHDD, BlockStoreLocation.anyDirInTier("MEM"));

    Assert.assertNotEquals(anyTier, BlockStoreLocation.anyDirInTier("HDD"));
    Assert.assertNotEquals(anyDirInTierMEM, BlockStoreLocation.anyDirInTier("HDD"));
    Assert.assertEquals(anyDirInTierHDD, BlockStoreLocation.anyDirInTier("HDD")); // Equals
    Assert.assertNotEquals(dirInMEM, BlockStoreLocation.anyDirInTier("HDD"));
    Assert.assertNotEquals(dirInHDD, BlockStoreLocation.anyDirInTier("HDD"));

    BlockStoreLocation loc1 = new BlockStoreLocation("MEM", 1);
    Assert.assertNotEquals(anyTier, loc1);
    Assert.assertNotEquals(anyDirInTierMEM, loc1);
    Assert.assertNotEquals(anyDirInTierHDD, loc1);
    Assert.assertEquals(dirInMEM, loc1); // Equals
    Assert.assertNotEquals(dirInHDD, loc1);

    BlockStoreLocation loc2 = new BlockStoreLocation("HDD", 2);
    Assert.assertNotEquals(anyTier, loc2);
    Assert.assertNotEquals(anyDirInTierMEM, loc2);
    Assert.assertNotEquals(anyDirInTierHDD, loc2);
    Assert.assertNotEquals(dirInMEM, loc2);
    Assert.assertEquals(dirInHDD, loc2); // Equals
  }
}
