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

import alluxio.Constants;

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
    String tierAlias = Constants.MEDIUM_SSD;
    int dirIndex = 3;
    String mediumType = Constants.MEDIUM_SSD;
    BlockStoreLocation loc = new BlockStoreLocation(tierAlias, dirIndex, mediumType);
    Assert.assertNotNull(loc);
    assertEquals(tierAlias, loc.tierAlias());
    assertEquals(dirIndex, loc.dir());
    assertEquals(mediumType, loc.mediumType());
  }

  /**
   * Tests the {@link BlockStoreLocation#belongsTo(BlockStoreLocation)} method.
   */
  @Test
  public void testBelongTo() {
    BlockStoreLocation anyTier = BlockStoreLocation.anyTier();
    BlockStoreLocation anyDirInTierMEM =
        BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM);
    BlockStoreLocation anyDirInTierHDD =
        BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD);
    BlockStoreLocation dirInMEM = new BlockStoreLocation(Constants.MEDIUM_MEM, 1);
    BlockStoreLocation dirInHDD = new BlockStoreLocation(Constants.MEDIUM_HDD, 2);
    BlockStoreLocation dirWithMediumType =
        new BlockStoreLocation(Constants.MEDIUM_MEM, 1, Constants.MEDIUM_MEM);
    BlockStoreLocation anyTierWithMEM =
        BlockStoreLocation.anyDirInAnyTierWithMedium(Constants.MEDIUM_MEM);

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

    assertTrue(dirWithMediumType.belongsTo(anyTierWithMEM));
    assertTrue(anyTierWithMEM.belongsTo(anyTier));
  }

  /**
   * Tests the {@link BlockStoreLocation#equals(Object)} method.
   */
  @Test
  public void equals() {
    BlockStoreLocation anyTier = BlockStoreLocation.anyTier();
    BlockStoreLocation anyDirInTierMEM =
        BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM);
    BlockStoreLocation anyDirInTierHDD =
        BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD);
    BlockStoreLocation dirInMEM = new BlockStoreLocation(Constants.MEDIUM_MEM, 1);
    BlockStoreLocation dirInHDD = new BlockStoreLocation(Constants.MEDIUM_HDD, 2);

    assertEquals(anyTier, BlockStoreLocation.anyTier()); // Equals
    assertNotEquals(anyDirInTierMEM, BlockStoreLocation.anyTier());
    assertNotEquals(anyDirInTierHDD, BlockStoreLocation.anyTier());
    assertNotEquals(dirInMEM, BlockStoreLocation.anyTier());
    assertNotEquals(dirInHDD, BlockStoreLocation.anyTier());

    assertNotEquals(anyTier, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM));
    assertEquals(anyDirInTierMEM, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM)); // Equals
    assertNotEquals(anyDirInTierHDD, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM));
    assertNotEquals(dirInMEM, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM));
    assertNotEquals(dirInHDD, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM));

    assertNotEquals(anyTier, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD));
    assertNotEquals(anyDirInTierMEM, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD));
    assertEquals(anyDirInTierHDD, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD)); // Equals
    assertNotEquals(dirInMEM, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD));
    assertNotEquals(dirInHDD, BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD));

    BlockStoreLocation loc1 = new BlockStoreLocation(Constants.MEDIUM_MEM, 1);
    assertNotEquals(anyTier, loc1);
    assertNotEquals(anyDirInTierMEM, loc1);
    assertNotEquals(anyDirInTierHDD, loc1);
    assertEquals(dirInMEM, loc1); // Equals
    assertNotEquals(dirInHDD, loc1);

    BlockStoreLocation loc2 = new BlockStoreLocation(Constants.MEDIUM_HDD, 2);
    assertNotEquals(anyTier, loc2);
    assertNotEquals(anyDirInTierMEM, loc2);
    assertNotEquals(anyDirInTierHDD, loc2);
    assertNotEquals(dirInMEM, loc2);
    assertEquals(dirInHDD, loc2); // Equals
  }
}
