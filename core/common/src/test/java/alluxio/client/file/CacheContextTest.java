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

import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;

import org.junit.Test;

public class CacheContextTest {

  @Test
  public void defaults() {
    CacheContext defaultContext = new CacheContext();
    assertEquals(CacheQuota.UNLIMITED, defaultContext.getCacheQuota());
    assertEquals(CacheScope.GLOBAL, defaultContext.getCacheScope());
    assertNull(defaultContext.getFileIdentifier());
  }

  @Test
  public void setters() {
    CacheContext context = new CacheContext()
        .setCacheQuota(new CacheQuota())
        .setCacheScope(CacheScope.create("db.table"))
        .setFileIdentifier("1234");
    assertEquals(new CacheQuota(), context.getCacheQuota());
    assertEquals(CacheScope.create("db.table"), context.getCacheScope());
    assertEquals("1234", context.getFileIdentifier());
  }
}
