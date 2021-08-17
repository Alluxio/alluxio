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

import static org.junit.Assert.assertThrows;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.PageNotFoundException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link DefaultMetaStore} class.
 */
public class DefaultMetaStoreTest {
  protected final PageId mPage = new PageId("1L", 2L);
  protected final PageInfo mPageInfo = new PageInfo(mPage, 1024);
  protected final InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  protected DefaultMetaStore mMetaStore;

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
    assertThrows(PageNotFoundException.class, () -> {
      Assert.assertEquals(mPageInfo, mMetaStore.removePage(mPage));
    });
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
    assertThrows(PageNotFoundException.class, () -> mMetaStore.getPageInfo(mPage));
  }

  @Test
  public void evict() throws Exception {
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertEquals(mPageInfo, mMetaStore.evict());
    mMetaStore.removePage(mPageInfo.getPageId());
    Assert.assertNull(mMetaStore.evict());
  }
}
