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

import tachyon.StorageLevelAlias;

public class BlockStoreLocationTest {

  @Test
  public void newLocationTest() throws Exception {
    StorageLevelAlias tierAlias = StorageLevelAlias.MEM;
    int tierLevel = 2;
    int dirIndex = 3;
    BlockStoreLocation loc = new BlockStoreLocation(tierAlias, tierLevel, dirIndex);
    Assert.assertNotNull(loc);
    Assert.assertEquals(tierAlias, loc.tierAlias());
    Assert.assertEquals(tierLevel, loc.tierLevel());
    Assert.assertEquals(dirIndex, loc.dir());
  }

  @Test
  public void testBelongTo() throws Exception {
    BlockStoreLocation anyTier = BlockStoreLocation.anyTier();
    BlockStoreLocation anyDirInTierMEM =
        BlockStoreLocation.anyDirInTier(0);
    BlockStoreLocation anyDirInTierHDD =
        BlockStoreLocation.anyDirInTier(1);
    BlockStoreLocation dirInMEM = new BlockStoreLocation(StorageLevelAlias.MEM.getValue(), 0, 1);
    BlockStoreLocation dirInHDD = new BlockStoreLocation(StorageLevelAlias.HDD.getValue(), 1, 2);

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

  @Test
  public void equalsTest() throws Exception {
    BlockStoreLocation anyTier = BlockStoreLocation.anyTier();
    BlockStoreLocation anyDirInTierMEM =
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.MEM.getValue());
    BlockStoreLocation anyDirInTierHDD =
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.HDD.getValue());
    BlockStoreLocation dirInMEM = new BlockStoreLocation(StorageLevelAlias.MEM.getValue(), 0, 1);
    BlockStoreLocation dirInHDD = new BlockStoreLocation(StorageLevelAlias.HDD.getValue(), 0, 2);

    Assert.assertEquals(anyTier, BlockStoreLocation.anyTier()); // Equals
    Assert.assertNotEquals(anyDirInTierMEM, BlockStoreLocation.anyTier());
    Assert.assertNotEquals(anyDirInTierHDD, BlockStoreLocation.anyTier());
    Assert.assertNotEquals(dirInMEM, BlockStoreLocation.anyTier());
    Assert.assertNotEquals(dirInHDD, BlockStoreLocation.anyTier());

    Assert.assertNotEquals(anyTier,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.MEM.getValue()));
    Assert.assertEquals(anyDirInTierMEM,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.MEM.getValue())); // Equals
    Assert.assertNotEquals(anyDirInTierHDD,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.MEM.getValue()));
    Assert.assertNotEquals(dirInMEM,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.MEM.getValue()));
    Assert.assertNotEquals(dirInHDD,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.MEM.getValue()));

    Assert.assertNotEquals(anyTier,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.HDD.getValue()));
    Assert.assertNotEquals(anyDirInTierMEM,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.HDD.getValue()));
    Assert.assertEquals(anyDirInTierHDD,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.HDD.getValue())); // Equals
    Assert.assertNotEquals(dirInMEM,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.HDD.getValue()));
    Assert.assertNotEquals(dirInHDD,
        BlockStoreLocation.anyDirInTier(StorageLevelAlias.HDD.getValue()));

    BlockStoreLocation loc1 = new BlockStoreLocation(StorageLevelAlias.MEM.getValue(), 0, 1);
    Assert.assertNotEquals(anyTier, loc1);
    Assert.assertNotEquals(anyDirInTierMEM, loc1);
    Assert.assertNotEquals(anyDirInTierHDD, loc1);
    Assert.assertEquals(dirInMEM, loc1); // Equals
    Assert.assertNotEquals(dirInHDD, loc1);

    BlockStoreLocation loc2 = new BlockStoreLocation(StorageLevelAlias.HDD.getValue(), 0, 2);
    Assert.assertNotEquals(anyTier, loc2);
    Assert.assertNotEquals(anyDirInTierMEM, loc2);
    Assert.assertNotEquals(anyDirInTierHDD, loc2);
    Assert.assertNotEquals(dirInMEM, loc2);
    Assert.assertEquals(dirInHDD, loc2); // Equals
  }
}
