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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link StorageTierAssoc}.
 */
public class StorageTierAssocTest {
  private void checkStorageTierAssoc(StorageTierAssoc assoc, String levelsProperty,
      String aliasFormat) {
    int size = Configuration.getInt(levelsProperty);
    Assert.assertEquals(size, assoc.size());

    List<String> expectedOrderedAliases = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      String alias = Configuration.get(String.format(aliasFormat, i));
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
  public void masterWorkerConfConstructorTest() {
    Configuration.set(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVELS, "4");
    Configuration.set(String.format(PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT, 3),
        "BOTTOM");
    Configuration.set(PropertyKey.WORKER_TIERED_STORE_LEVELS, "2");
    Configuration
        .set(String.format(PropertyKey.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 1), "BOTTOM");

    checkStorageTierAssoc(new MasterStorageTierAssoc(),
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVELS,
        PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT);
    checkStorageTierAssoc(new WorkerStorageTierAssoc(), PropertyKey.WORKER_TIERED_STORE_LEVELS,
        PropertyKey.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT);
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests the constructors of the {@link MasterStorageTierAssoc} and {@link WorkerStorageTierAssoc}
   * classes with different storage alias.
   */
  @Test
  public void storageAliasListConstructorTest() {
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
}
