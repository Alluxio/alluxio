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

package alluxio.client.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import alluxio.client.hive.HiveCacheContext;
import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;

import org.junit.Test;

public class CacheContextTest {

  @Test
  public void defaults() {
    CacheContext defaultContext = CacheContext.defaults();
    assertEquals(CacheQuota.UNLIMITED, defaultContext.getCacheQuota());
    assertEquals(CacheScope.GLOBAL, defaultContext.getCacheScope());
    assertNull(defaultContext.getCacheIdentifier());
    assertNull(defaultContext.getHiveCacheContext());
  }

  @Test
  public void setters() {
    CacheContext context = CacheContext.defaults()
        .setCacheQuota(new CacheQuota())
        .setCacheScope(CacheScope.create("db.table"))
        .setCacheIdentifier("1234")
        .setHiveCacheContext(new HiveCacheContext("db", "tb", "partition"));
    assertEquals(new CacheQuota(), context.getCacheQuota());
    assertEquals(CacheScope.create("db.table"), context.getCacheScope());
    assertEquals("1234", context.getCacheIdentifier());
    assertEquals(new HiveCacheContext("db", "tb", "partition"), context.getHiveCacheContext());
  }
}
