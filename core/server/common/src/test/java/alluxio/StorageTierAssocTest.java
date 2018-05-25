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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link StorageTierAssoc}.
 */
public final class StorageTierAssocTest {
  private void checkStorageTierAssoc(StorageTierAssoc assoc, PropertyKey levelsProperty,
      PropertyKey.Template template) {
    int size = Configuration.getInt(levelsProperty);
    Assert.assertEquals(size, assoc.size());

    List<String> expectedOrderedAliases = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      String alias = Configuration.get(template.format(i));
      Assert.assertEquals(i, assoc.getOrdinal(alias));
      Assert.assertEquals(alias, assoc.getAlias(i));
      expectedOrderedAliases.add(alias);
    }

    Assert.assertEquals(expectedOrderedAliases, assoc.getOrderedStorageAliases());
  }

  /**
   * Tests the constructors of the {@link MasterStorageTierAssoc} and {@link WorkerStorageTierAssoc}
   * classes with a {@link Configuration}.
   */
  @Test
  public void masterWorkerConfConstructor() throws Exception {
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVELS, "3",
        PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS.format(2), "BOTTOM",
        PropertyKey.WORKER_TIERED_STORE_LEVELS, "2",
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(1), "BOTTOM"))
        .toResource()) {
      checkStorageTierAssoc(new MasterStorageTierAssoc(),
          PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVELS,
          PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS);
      checkStorageTierAssoc(new WorkerStorageTierAssoc(), PropertyKey.WORKER_TIERED_STORE_LEVELS,
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS);
    }
  }

  /**
   * Tests the constructors of the {@link MasterStorageTierAssoc} and {@link WorkerStorageTierAssoc}
   * classes with different storage alias.
   */
  @Test
  public void storageAliasListConstructor() {
    List<String> orderedAliases = Arrays.asList("MEM", "HDD", "SOMETHINGELSE", "SSD");

    MasterStorageTierAssoc masterAssoc = new MasterStorageTierAssoc(orderedAliases);
    WorkerStorageTierAssoc workerAssoc = new WorkerStorageTierAssoc(orderedAliases);

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
  }

  @Test
  public void interpretTier() throws Exception {
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(0, 1));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(1, 1));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(2, 1));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(-1, 1));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(-2, 1));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(-3, 1));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(Constants.FIRST_TIER, 1));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(Constants.SECOND_TIER, 1));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(Constants.LAST_TIER, 1));

    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(0, 10));
    Assert.assertEquals(8, StorageTierAssoc.interpretOrdinal(8, 10));
    Assert.assertEquals(9, StorageTierAssoc.interpretOrdinal(9, 10));
    Assert.assertEquals(9, StorageTierAssoc.interpretOrdinal(10, 10));
    Assert.assertEquals(9, StorageTierAssoc.interpretOrdinal(-1, 10));
    Assert.assertEquals(1, StorageTierAssoc.interpretOrdinal(-9, 10));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(-10, 10));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(-11, 10));
    Assert.assertEquals(0, StorageTierAssoc.interpretOrdinal(Constants.FIRST_TIER, 10));
    Assert.assertEquals(1, StorageTierAssoc.interpretOrdinal(Constants.SECOND_TIER, 10));
    Assert.assertEquals(9, StorageTierAssoc.interpretOrdinal(Constants.LAST_TIER, 10));
  }
}
