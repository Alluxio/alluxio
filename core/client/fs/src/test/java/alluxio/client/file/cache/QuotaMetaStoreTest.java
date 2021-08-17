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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.client.quota.CacheScope;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link QuotaMetaStore} class.
 */
public class QuotaMetaStoreTest extends DefaultMetaStoreTest {
  private QuotaMetaStore mQuotaMetaStore;
  private final CacheScope mPartitionScope = CacheScope.create("schema.table.partition");
  private final CacheScope mTableScope = CacheScope.create("schema.table");
  private final CacheScope mSchemaScope = CacheScope.create("schema");
  private final long mPageSize = 8765;

  @Before
  public void before() {
    mMetaStore = new QuotaMetaStore(mConf);
    mQuotaMetaStore = (QuotaMetaStore) mMetaStore;
  }

  @Test
  public void evictInScope() throws Exception {
    assertNull(mQuotaMetaStore.evict(CacheScope.GLOBAL));
    PageInfo pageInfo = new PageInfo(mPage, mPageSize, mSchemaScope);
    mQuotaMetaStore.addPage(mPage, pageInfo);
    assertNull(mQuotaMetaStore.evict(mPartitionScope));
    assertNull(mQuotaMetaStore.evict(mTableScope));
    assertEquals(pageInfo, mQuotaMetaStore.evict(mSchemaScope));
    assertEquals(pageInfo, mQuotaMetaStore.evict(CacheScope.GLOBAL));
  }

  @Test
  public void evictInScope2() throws Exception {
    CacheScope partitionScope1 = CacheScope.create("schema.table.partition1");
    CacheScope partitionScope2 = CacheScope.create("schema.table.partition2");
    PageId pageId1 = new PageId("1L", 2L);
    PageId pageId2 = new PageId("3L", 4L);
    PageInfo pageInfo1 = new PageInfo(pageId1, 1234, partitionScope1);
    PageInfo pageInfo2 = new PageInfo(pageId2, 5678, partitionScope2);
    mQuotaMetaStore.addPage(pageId1, pageInfo1);
    mQuotaMetaStore.addPage(pageId2, pageInfo2);
    assertEquals(pageInfo1, mQuotaMetaStore.evict(partitionScope1));
    assertEquals(pageInfo2, mQuotaMetaStore.evict(partitionScope2));
    PageInfo evicted = mQuotaMetaStore.evict(mTableScope);
    assertTrue(evicted == pageInfo1 || evicted == pageInfo2);
    evicted = mQuotaMetaStore.evict(mSchemaScope);
    assertTrue(evicted == pageInfo1 || evicted == pageInfo2);
    evicted = mQuotaMetaStore.evict(CacheScope.GLOBAL);
    assertTrue(evicted == pageInfo1 || evicted == pageInfo2);
    mQuotaMetaStore.removePage(pageId1);
    assertNull(mQuotaMetaStore.evict(partitionScope1));
    assertEquals(pageInfo2, mQuotaMetaStore.evict(partitionScope2));
    assertEquals(pageInfo2, mQuotaMetaStore.evict(mTableScope));
    assertEquals(pageInfo2, mQuotaMetaStore.evict(mSchemaScope));
    assertEquals(pageInfo2, mQuotaMetaStore.evict(CacheScope.GLOBAL));
  }

  @Test
  public void bytesInScope() throws Exception {
    PageInfo pageInfo = new PageInfo(mPage, mPageSize, mPartitionScope);
    mQuotaMetaStore.addPage(mPage, pageInfo);
    assertEquals(mPageSize, mQuotaMetaStore.bytes(mPartitionScope));
    assertEquals(mPageSize, mQuotaMetaStore.bytes(mTableScope));
    assertEquals(mPageSize, mQuotaMetaStore.bytes(mSchemaScope));
    assertEquals(mPageSize, mQuotaMetaStore.bytes(CacheScope.GLOBAL));
    mQuotaMetaStore.removePage(mPage);
    assertEquals(0, mQuotaMetaStore.bytes(mPartitionScope));
    assertEquals(0, mQuotaMetaStore.bytes(mTableScope));
    assertEquals(0, mQuotaMetaStore.bytes(mSchemaScope));
    assertEquals(0, mQuotaMetaStore.bytes(CacheScope.GLOBAL));
  }
}
