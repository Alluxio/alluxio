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

package alluxio.client.file.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CacheManagerTest {
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Test
  public void factoryGet() throws Exception {
    CacheManager manager = CacheManager.Factory.get(mConf);
    assertEquals(manager, CacheManager.Factory.get(mConf));
  }

  @Test
  public void factoryGetConcurrently() throws Exception {
    CacheManager.Factory.clear();
    int cnt = 10;
    List<Future<CacheManager>> cacheManagersList = new ArrayList<>(cnt);
    ExecutorService executorService = Executors.newFixedThreadPool(cnt);
    for (int i = 0; i < cnt; i++) {
      cacheManagersList.add(executorService.submit(() -> {
        try {
          return CacheManager.Factory.get(mConf);
        } catch (Exception e) {
          return null;
        }
      }));
    }

    CacheManager checkedCacheManager = CacheManager.Factory.get(mConf);
    for (Future<CacheManager> f : cacheManagersList) {
      CacheManager cacheManager = f.get();
      assertNotNull(cacheManager);
      assertEquals(cacheManager, checkedCacheManager);
    }
  }

  @Test
  public void factoryCreate() throws Exception {
    CacheManager manager = CacheManager.Factory.create(mConf);
    assertNotEquals(manager, CacheManager.Factory.create(mConf));
  }

  @Test
  public void factoryClear() throws Exception {
    CacheManager manager = CacheManager.Factory.get(mConf);
    CacheManager.Factory.clear();
    assertNotEquals(manager, CacheManager.Factory.get(mConf));
  }
}
