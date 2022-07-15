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

import alluxio.client.file.cache.store.LocalPageStoreOptions;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.PageNotFoundException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Gauge;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;

/**
 * Tests for the {@link DefaultPageMetaStore} class.
 */
public class DefaultMetaStoreTest {
  protected final PageId mPage = new PageId("1L", 2L);
  protected final AlluxioConfiguration mConf = Configuration.global();
  protected PageStoreDir mPageStoreDir;
  protected PageInfo mPageInfo;
  protected DefaultPageMetaStore mMetaStore;
  protected Gauge mCachedPageGauge;

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  /**
   * Sets up the instances.
   */
  @Before
  public void before() {
    MetricsSystem.clearAllMetrics();
    mPageStoreDir =
        PageStoreDir.createPageStoreDir(mConf,
            new LocalPageStoreOptions().setRootDir(
                Paths.get(mTempFolder.getRoot().getAbsolutePath())));
    mPageInfo = new PageInfo(mPage, 1024,
        mPageStoreDir);
    mMetaStore = new DefaultPageMetaStore();
    mCachedPageGauge =
        MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricKey.CLIENT_CACHE_PAGES.getName());
  }

  @Test
  public void addNew() {
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
    Assert.assertEquals(1, mCachedPageGauge.getValue());
  }

  @Test
  public void addExist() {
    mMetaStore.addPage(mPage, mPageInfo);
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
    Assert.assertEquals(1, mCachedPageGauge.getValue());
  }

  @Test
  public void removeExist() throws Exception {
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
    mMetaStore.removePage(mPage);
    Assert.assertFalse(mMetaStore.hasPage(mPage));
    Assert.assertEquals(0, mCachedPageGauge.getValue());
  }

  @Test
  public void removeNotExist() throws Exception {
    assertThrows(PageNotFoundException.class, () -> {
      Assert.assertEquals(mPageInfo, mMetaStore.removePage(mPage));
    });
    Assert.assertEquals(0, mCachedPageGauge.getValue());
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
    Assert.assertEquals(mPageInfo, mMetaStore.evict(mPageStoreDir));
    mMetaStore.removePage(mPageInfo.getPageId());
    Assert.assertNull(mMetaStore.evict(mPageStoreDir));
    Assert.assertEquals(0, mCachedPageGauge.getValue());
  }
}
