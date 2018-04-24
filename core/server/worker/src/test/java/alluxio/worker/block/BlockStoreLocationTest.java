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

package alluxio.worker.block;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link BlockStoreLocation}.
 */
public final class BlockStoreLocationTest {

  /**
   * Tests that a new location can be created with the constructor.
   */
  @Test
  public void newLocation() {
    String tierAlias = "SSD";
    int dirIndex = 3;
    BlockStoreLocation loc = new BlockStoreLocation(tierAlias, dirIndex);
    Assert.assertNotNull(loc);
    assertEquals(tierAlias, loc.tierAlias());
    assertEquals(dirIndex, loc.dir());
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

    assertTrue(anyTier.belongsTo(anyTier));
    assertFalse(anyTier.belongsTo(anyDirInTierMEM));
    assertFalse(anyTier.belongsTo(anyDirInTierHDD));
    assertFalse(anyTier.belongsTo(dirInMEM));
    assertFalse(anyTier.belongsTo(dirInHDD));

    assertTrue(anyDirInTierMEM.belongsTo(anyTier));
    assertTrue(anyDirInTierMEM.belongsTo(anyDirInTierMEM));
    assertFalse(anyDirInTierMEM.belongsTo(anyDirInTierHDD));
    assertFalse(anyDirInTierMEM.belongsTo(dirInMEM));
    assertFalse(anyDirInTierMEM.belongsTo(dirInHDD));

    assertTrue(anyDirInTierHDD.belongsTo(anyTier));
    assertFalse(anyDirInTierHDD.belongsTo(anyDirInTierMEM));
    assertTrue(anyDirInTierHDD.belongsTo(anyDirInTierHDD));
    assertFalse(anyDirInTierHDD.belongsTo(dirInMEM));
    assertFalse(anyDirInTierHDD.belongsTo(dirInHDD));

    assertTrue(dirInMEM.belongsTo(anyTier));
    assertTrue(dirInMEM.belongsTo(anyDirInTierMEM));
    assertFalse(dirInMEM.belongsTo(anyDirInTierHDD));
    assertTrue(dirInMEM.belongsTo(dirInMEM));
    assertFalse(dirInMEM.belongsTo(dirInHDD));

    assertTrue(dirInHDD.belongsTo(anyTier));
    assertFalse(dirInHDD.belongsTo(anyDirInTierMEM));
    assertTrue(dirInHDD.belongsTo(anyDirInTierHDD));
    assertFalse(dirInHDD.belongsTo(dirInMEM));
    assertTrue(dirInHDD.belongsTo(dirInHDD));
  }

  /**
   * Tests the {@link BlockStoreLocation#equals(Object)} method.
   */
  @Test
  public void equals() {
    BlockStoreLocation anyTier = BlockStoreLocation.anyTier();
    BlockStoreLocation anyDirInTierMEM =
        BlockStoreLocation.anyDirInTier("MEM");
    BlockStoreLocation anyDirInTierHDD =
        BlockStoreLocation.anyDirInTier("HDD");
    BlockStoreLocation dirInMEM = new BlockStoreLocation("MEM", 1);
    BlockStoreLocation dirInHDD = new BlockStoreLocation("HDD", 2);

    assertEquals(anyTier, BlockStoreLocation.anyTier()); // Equals
    assertNotEquals(anyDirInTierMEM, BlockStoreLocation.anyTier());
    assertNotEquals(anyDirInTierHDD, BlockStoreLocation.anyTier());
    assertNotEquals(dirInMEM, BlockStoreLocation.anyTier());
    assertNotEquals(dirInHDD, BlockStoreLocation.anyTier());

    assertNotEquals(anyTier, BlockStoreLocation.anyDirInTier("MEM"));
    assertEquals(anyDirInTierMEM, BlockStoreLocation.anyDirInTier("MEM")); // Equals
    assertNotEquals(anyDirInTierHDD, BlockStoreLocation.anyDirInTier("MEM"));
    assertNotEquals(dirInMEM, BlockStoreLocation.anyDirInTier("MEM"));
    assertNotEquals(dirInHDD, BlockStoreLocation.anyDirInTier("MEM"));

    assertNotEquals(anyTier, BlockStoreLocation.anyDirInTier("HDD"));
    assertNotEquals(anyDirInTierMEM, BlockStoreLocation.anyDirInTier("HDD"));
    assertEquals(anyDirInTierHDD, BlockStoreLocation.anyDirInTier("HDD")); // Equals
    assertNotEquals(dirInMEM, BlockStoreLocation.anyDirInTier("HDD"));
    assertNotEquals(dirInHDD, BlockStoreLocation.anyDirInTier("HDD"));

    BlockStoreLocation loc1 = new BlockStoreLocation("MEM", 1);
    assertNotEquals(anyTier, loc1);
    assertNotEquals(anyDirInTierMEM, loc1);
    assertNotEquals(anyDirInTierHDD, loc1);
    assertEquals(dirInMEM, loc1); // Equals
    assertNotEquals(dirInHDD, loc1);

    BlockStoreLocation loc2 = new BlockStoreLocation("HDD", 2);
    assertNotEquals(anyTier, loc2);
    assertNotEquals(anyDirInTierMEM, loc2);
    assertNotEquals(anyDirInTierHDD, loc2);
    assertNotEquals(dirInMEM, loc2);
    assertEquals(dirInHDD, loc2); // Equals
  }
}
