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
  private DefaultMetaStore mMetaStore;

  /**
   * Sets up the instances.
   */
  @Before
  public void before() {
    mMetaStore = new DefaultMetaStore();
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
}
