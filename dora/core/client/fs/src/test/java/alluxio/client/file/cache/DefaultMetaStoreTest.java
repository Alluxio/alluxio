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

import static alluxio.client.file.cache.CacheUsage.PartitionDescriptor.dir;
import static alluxio.client.file.cache.CacheUsage.PartitionDescriptor.file;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import alluxio.Constants;
import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.MemoryPageStore;
import alluxio.client.file.cache.store.MemoryPageStoreDir;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.PageNotFoundException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Gauge;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

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
        PageStoreDir.createPageStoreDir(
            new CacheEvictorOptions().setEvictorClass(FIFOCacheEvictor.class),
            new PageStoreOptions().setRootDir(
                Paths.get(mTempFolder.getRoot().getAbsolutePath())));
    mPageInfo = new PageInfo(mPage, 1024,
        mPageStoreDir);
    mMetaStore = new DefaultPageMetaStore(ImmutableList.of(mPageStoreDir));
    mCachedPageGauge =
        MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricKey.CLIENT_CACHE_PAGES.getName());
  }

  @Test
  public void addNew() {
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
    assertEquals(1, mCachedPageGauge.getValue());
  }

  @Test
  public void addExist() {
    mMetaStore.addPage(mPage, mPageInfo);
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
    assertEquals(1, mCachedPageGauge.getValue());
  }

  @Test
  public void removeExist() throws Exception {
    mMetaStore.addPage(mPage, mPageInfo);
    Assert.assertTrue(mMetaStore.hasPage(mPage));
    mMetaStore.removePage(mPage);
    Assert.assertFalse(mMetaStore.hasPage(mPage));
    assertEquals(0, mCachedPageGauge.getValue());
  }

  @Test
  public void removeNotExist() throws Exception {
    assertThrows(PageNotFoundException.class, () -> {
      assertEquals(mPageInfo, mMetaStore.removePage(mPage));
    });
    assertEquals(0, mCachedPageGauge.getValue());
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
    assertEquals(mPageInfo, mMetaStore.getPageInfo(mPage));
  }

  @Test
  public void getPageInfoNotExist() throws Exception {
    assertThrows(PageNotFoundException.class, () -> mMetaStore.getPageInfo(mPage));
  }

  @Test
  public void evict() throws Exception {
    mMetaStore.addPage(mPage, mPageInfo);
    assertEquals(mPageInfo, mMetaStore.evict(mPageStoreDir));
    mMetaStore.removePage(mPageInfo.getPageId());
    Assert.assertNull(mMetaStore.evict(mPageStoreDir));
    assertEquals(0, mCachedPageGauge.getValue());
  }

  @Test
  public void cacheUsage() {
    PageId page1 = new PageId("0", 0);
    PageInfo pageInfo1 = new PageInfo(page1, 1024, mPageStoreDir);
    mMetaStore.addPage(page1, pageInfo1);
    Optional<CacheUsage> cacheUsage = mMetaStore.getUsage();
    long capacity = mPageStoreDir.getCapacityBytes();
    assertEquals(Optional.of(capacity),
        cacheUsage.map(CacheUsage::capacity));
    assertEquals(Optional.of(pageInfo1.getPageSize()),
        cacheUsage.map(CacheUsageView::used));
    assertEquals(Optional.of(capacity - pageInfo1.getPageSize()),
        cacheUsage.map(CacheUsageView::available));

    PageId page2 = new PageId("1", 0);
    PageInfo pageInfo2 = new PageInfo(page2, 2048, mPageStoreDir);
    mMetaStore.addPage(page2, pageInfo2);
    assertEquals(Optional.of(mPageStoreDir.getCapacityBytes()),
        cacheUsage.map(CacheUsage::capacity));
    assertEquals(Optional.of(pageInfo1.getPageSize() + pageInfo2.getPageSize()),
        cacheUsage.map(CacheUsageView::used));
    assertEquals(Optional.of(capacity - pageInfo1.getPageSize() - pageInfo2.getPageSize()),
        cacheUsage.map(CacheUsageView::available));
  }

  @Test
  public void fileCacheUsage() {
    PageId page1 = new PageId("0", 0);
    PageInfo pageInfo1 = new PageInfo(page1, Constants.KB, mPageStoreDir);
    mMetaStore.addPage(page1, pageInfo1);
    final int numPagesOfFile1 = 5;
    for (int i = 0; i < numPagesOfFile1; i++) {
      PageId page = new PageId("1", i);
      PageInfo pageInfo = new PageInfo(page, Constants.KB, mPageStoreDir);
      mMetaStore.addPage(page, pageInfo);
    }
    Optional<CacheUsage> globalUsage = mMetaStore.getUsage();
    Optional<CacheUsage> file0Usage = globalUsage
        .flatMap(usage -> usage.partitionedBy(file("0")));
    assertEquals(Optional.of((long) Constants.KB),
        file0Usage.map(CacheUsage::used));
    Optional<CacheUsage> file1Usage = globalUsage
        .flatMap(usage -> usage.partitionedBy(file("1")));
    assertEquals(Optional.of((long) (Constants.KB * numPagesOfFile1)),
        file1Usage.map(CacheUsage::used));
  }

  @Test
  public void dirCacheUsage() {
    PageStoreOptions options = new PageStoreOptions()
        .setPageSize(Constants.KB);
    List<PageStoreDir> dirs = ImmutableList.of(
        new MemoryPageStoreDir(options.setIndex(0).setCacheSize(Constants.MB),
            new MemoryPageStore((int) options.getPageSize()),
            new FIFOCacheEvictor(new CacheEvictorOptions())),
        new MemoryPageStoreDir(options.setIndex(1).setPageSize(Constants.MB * 2),
            new MemoryPageStore((int) options.getPageSize()),
            new FIFOCacheEvictor(new CacheEvictorOptions()))
    );
    PageMetaStore metaStore = new DefaultPageMetaStore(dirs);
    PageId page1 = new PageId("0", 0);
    PageInfo pageInfo1 = new PageInfo(page1, Constants.KB, dirs.get(0));
    metaStore.addPage(page1, pageInfo1);
    PageId page2 = new PageId("0", 1);
    PageInfo pageInfo2 = new PageInfo(page2, Constants.KB * 2, dirs.get(1));
    metaStore.addPage(page2, pageInfo2);

    Optional<CacheUsage> dir0Usage = metaStore.getUsage()
        .flatMap(usage -> usage.partitionedBy(dir(0)));
    assertEquals(Optional.of(pageInfo1.getPageSize()),
        dir0Usage.map(CacheUsage::used));
    assertEquals(Optional.of(dirs.get(0).getCapacityBytes()),
        dir0Usage.map(CacheUsage::capacity));
    assertEquals(Optional.of(dirs.get(0).getCapacityBytes() - pageInfo1.getPageSize()),
        dir0Usage.map(CacheUsage::available));

    Optional<CacheUsage> dir1Usage = metaStore.getUsage()
        .flatMap(usage -> usage.partitionedBy(dir(1)));
    assertEquals(Optional.of(pageInfo2.getPageSize()),
        dir1Usage.map(CacheUsage::used));
    assertEquals(Optional.of(dirs.get(1).getCapacityBytes()),
        dir1Usage.map(CacheUsage::capacity));
    assertEquals(Optional.of(dirs.get(1).getCapacityBytes() - pageInfo2.getPageSize()),
        dir1Usage.map(CacheUsage::available));
  }
}
