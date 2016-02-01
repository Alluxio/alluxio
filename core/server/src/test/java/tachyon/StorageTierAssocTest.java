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

package tachyon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tachyon.conf.TachyonConf;

/**
 * Unit tests for {@link StorageTierAssoc}.
 */
public class StorageTierAssocTest {
  private void checkStorageTierAssoc(StorageTierAssoc assoc, TachyonConf conf,
      String levelsProperty, String aliasFormat) {
    int size = conf.getInt(levelsProperty);
    Assert.assertEquals(size, assoc.size());

    List<String> expectedOrderedAliases = new ArrayList<String>();

    for (int i = 0; i < size; i ++) {
      String alias = conf.get(String.format(aliasFormat, i));
      Assert.assertEquals(i, assoc.getOrdinal(alias));
      Assert.assertEquals(alias, assoc.getAlias(i));
      expectedOrderedAliases.add(alias);
    }

    Assert.assertEquals(expectedOrderedAliases, assoc.getOrderedStorageAliases());
  }

  /**
   * Tests the constructors of the {@link MasterStorageTierAssoc} and {@link WorkerStorageTierAssoc}
   * classes with a {@link TachyonConf}.
   */
  @Test
  public void masterWorkerConfConstructorTest() {
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.MASTER_TIERED_STORE_GLOBAL_LEVELS, "4");
    tachyonConf.set(String.format(Constants.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT, 3),
        "BOTTOM");
    tachyonConf.set(Constants.WORKER_TIERED_STORE_LEVELS, "2");
    tachyonConf
        .set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 1), "BOTTOTM");

    checkStorageTierAssoc(new MasterStorageTierAssoc(tachyonConf), tachyonConf,
        Constants.MASTER_TIERED_STORE_GLOBAL_LEVELS,
        Constants.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT);
    checkStorageTierAssoc(new WorkerStorageTierAssoc(tachyonConf), tachyonConf,
        Constants.WORKER_TIERED_STORE_LEVELS, Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT);
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
    for (int i = 0; i < orderedAliases.size(); i ++) {
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
