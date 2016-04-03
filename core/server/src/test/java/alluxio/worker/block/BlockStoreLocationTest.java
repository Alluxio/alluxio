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

package alluxio.worker.block;

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
   * Tests the {@link BlockStoreLocation#belongsTo(BlockStoreLocation)} method.
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

    Assert.assertTrue(anyTier.belongsTo(anyTier));
    Assert.assertFalse(anyTier.belongsTo(anyDirInTierMEM));
    Assert.assertFalse(anyTier.belongsTo(anyDirInTierHDD));
    Assert.assertFalse(anyTier.belongsTo(dirInMEM));
    Assert.assertFalse(anyTier.belongsTo(dirInHDD));

    Assert.assertTrue(anyDirInTierMEM.belongsTo(anyTier));
    Assert.assertTrue(anyDirInTierMEM.belongsTo(anyDirInTierMEM));
    Assert.assertFalse(anyDirInTierMEM.belongsTo(anyDirInTierHDD));
    Assert.assertFalse(anyDirInTierMEM.belongsTo(dirInMEM));
    Assert.assertFalse(anyDirInTierMEM.belongsTo(dirInHDD));

    Assert.assertTrue(anyDirInTierHDD.belongsTo(anyTier));
    Assert.assertFalse(anyDirInTierHDD.belongsTo(anyDirInTierMEM));
    Assert.assertTrue(anyDirInTierHDD.belongsTo(anyDirInTierHDD));
    Assert.assertFalse(anyDirInTierHDD.belongsTo(dirInMEM));
    Assert.assertFalse(anyDirInTierHDD.belongsTo(dirInHDD));

    Assert.assertTrue(dirInMEM.belongsTo(anyTier));
    Assert.assertTrue(dirInMEM.belongsTo(anyDirInTierMEM));
    Assert.assertFalse(dirInMEM.belongsTo(anyDirInTierHDD));
    Assert.assertTrue(dirInMEM.belongsTo(dirInMEM));
    Assert.assertFalse(dirInMEM.belongsTo(dirInHDD));

    Assert.assertTrue(dirInHDD.belongsTo(anyTier));
    Assert.assertFalse(dirInHDD.belongsTo(anyDirInTierMEM));
    Assert.assertTrue(dirInHDD.belongsTo(anyDirInTierHDD));
    Assert.assertFalse(dirInHDD.belongsTo(dirInMEM));
    Assert.assertTrue(dirInHDD.belongsTo(dirInHDD));
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
