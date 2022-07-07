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

package alluxio;

import alluxio.collections.Pair;
import alluxio.worker.block.BlockStoreLocation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link DefaultStorageTierAssoc}.
 */
public final class DefaultStorageTierAssocTest {
  /**
   * Tests the constructors of the {@link DefaultStorageTierAssoc} classes with different storage
   * alias.
   */
  @Test
  public void storageAliasListConstructor() {
    List<String> orderedAliases = Arrays.asList(Constants.MEDIUM_MEM,
        Constants.MEDIUM_HDD, "SOMETHINGELSE", Constants.MEDIUM_SSD);

    StorageTierAssoc masterAssoc = new DefaultStorageTierAssoc(orderedAliases);
    StorageTierAssoc workerAssoc = new DefaultStorageTierAssoc(orderedAliases);

    Assert.assertEquals(orderedAliases.size(), masterAssoc.size());
    Assert.assertEquals(orderedAliases.size(), workerAssoc.size());
    for (int i = 0; i < orderedAliases.size(); i++) {
      String alias = orderedAliases.get(i);
      Assert.assertEquals(alias, masterAssoc.getAlias(i));
      Assert.assertEquals(i, masterAssoc.getOrdinal(alias));
      Assert.assertEquals(alias, workerAssoc.getAlias(i));
      Assert.assertEquals(i, workerAssoc.getOrdinal(alias));
    }

    Assert.assertEquals(orderedAliases, masterAssoc.getOrderedStorageAliases());
    Assert.assertEquals(orderedAliases, workerAssoc.getOrderedStorageAliases());

    // Validate intersections.
    List<Pair<BlockStoreLocation, BlockStoreLocation>> intersections =
        workerAssoc.intersectionList();
    Assert.assertEquals(intersections.get(0).getFirst(),
        BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM));
    Assert.assertEquals(intersections.get(0).getSecond(),
        BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD));
    Assert.assertEquals(intersections.get(1).getFirst(),
        BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD));
    Assert.assertEquals(intersections.get(1).getSecond(),
        BlockStoreLocation.anyDirInTier("SOMETHINGELSE"));
    Assert.assertEquals(intersections.get(2).getFirst(),
        BlockStoreLocation.anyDirInTier("SOMETHINGELSE"));
    Assert.assertEquals(intersections.get(2).getSecond(),
        BlockStoreLocation.anyDirInTier(Constants.MEDIUM_SSD));
  }

  @Test
  public void interpretTier() throws Exception {
    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(0, 1));
    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(1, 1));
    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(2, 1));
    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(-1, 1));
    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(-2, 1));
    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(-3, 1));
    Assert.assertEquals(0,
        DefaultStorageTierAssoc.interpretOrdinal(Constants.FIRST_TIER, 1));
    Assert.assertEquals(0,
        DefaultStorageTierAssoc.interpretOrdinal(Constants.SECOND_TIER, 1));
    Assert.assertEquals(0,
        DefaultStorageTierAssoc.interpretOrdinal(Constants.LAST_TIER, 1));

    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(0, 10));
    Assert.assertEquals(8, DefaultStorageTierAssoc.interpretOrdinal(8, 10));
    Assert.assertEquals(9, DefaultStorageTierAssoc.interpretOrdinal(9, 10));
    Assert.assertEquals(9, DefaultStorageTierAssoc.interpretOrdinal(10, 10));
    Assert.assertEquals(9, DefaultStorageTierAssoc.interpretOrdinal(-1, 10));
    Assert.assertEquals(1, DefaultStorageTierAssoc.interpretOrdinal(-9, 10));
    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(-10, 10));
    Assert.assertEquals(0, DefaultStorageTierAssoc.interpretOrdinal(-11, 10));
    Assert.assertEquals(0,
        DefaultStorageTierAssoc.interpretOrdinal(Constants.FIRST_TIER, 10));
    Assert.assertEquals(1,
        DefaultStorageTierAssoc.interpretOrdinal(Constants.SECOND_TIER, 10));
    Assert.assertEquals(9,
        DefaultStorageTierAssoc.interpretOrdinal(Constants.LAST_TIER, 10));
  }
}
