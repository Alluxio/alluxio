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

import alluxio.client.quota.CacheScope;
import alluxio.conf.InstancedConfiguration;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for the {@link DefaultMetaStore} class.
 */
public final class DefaultMetaStoreTest {
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private final PageId mPage = new PageId("1L", 2L);
  private final PageInfo mPageInfo = new PageInfo(mPage, 1024);
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  private DefaultMetaStore mMetaStore;

  /**
   * Sets up the instances.
   */
  @Before
  public void before() {
    mMetaStore = new DefaultMetaStore(mConf);
  }

  @Test
  public void addNew() {
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
  }

  @Test
  public void addExist() {
    mMetaStore.addPage(mPage, mPageInfo);
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
  }

  @Test
  public void removeExist() throws Exception {
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
    mMetaStore.removePage(mPage);
    Assert.assertFalse(mMetaStore.hasPage(mPage));
  }

  @Test
  public void removeNotExist() throws Exception {
    mThrown.expect(PageNotFoundException.class);
    mMetaStore.removePage(mPage);
  }

  @Test
  public void hasPage() {
    Assert.assertFalse(mMetaStore.hasPage(mPage));
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
  }

  @Test
  public void getPageInfo() throws Exception {
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertEquals(mPageInfo, mMetaStore.getPageInfo(mPage));
  }

  @Test
  public void getPageInfoNotExist() throws Exception {
    mThrown.expect(PageNotFoundException.class);
    mMetaStore.getPageInfo(mPage);
  }

  @Test
  public void evict() throws Exception {
    Assert.assertNull(mMetaStore.evict(CacheScope.GLOBAL));
  }

  @Test
  public void bytesInScope() {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, true);
    mMetaStore = new DefaultMetaStore(mConf);
    long pageSize1 = 8765;
    PageId pageId = new PageId("2L", 2L);
    CacheScope cacheScope = CacheScope.create("schema.table.partition");
    PageInfo pageInfo = new PageInfo(pageId, pageSize1, cacheScope);
    mMetaStore.addPage(pageId, pageInfo);
    Assert.assertEquals(pageSize1, mMetaStore.bytes(CacheScope.create("schema.table.partition")));
    Assert.assertEquals(pageSize1, mMetaStore.bytes(CacheScope.create("schema.table")));
    Assert.assertEquals(pageSize1, mMetaStore.bytes(CacheScope.create("schema")));
    Assert.assertEquals(pageSize1, mMetaStore.bytes(CacheScope.create(".")));
  }
}
