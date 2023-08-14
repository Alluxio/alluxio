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

package alluxio.client.file.cache.store;

import static alluxio.client.file.cache.CacheUsage.PartitionDescriptor.file;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.ProjectConstants;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.cache.CacheUsageView;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@RunWith(Parameterized.class)
public class PageStoreDirTest {
  public static final long CACHE_CAPACITY = 65536;
  public static final long PAGE_SIZE = 1024;
  private final AlluxioConfiguration mConf = Configuration.global();

  @Parameterized.Parameters(name = "{index}-{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {PageStoreType.LOCAL},
        {PageStoreType.MEM}
    });
  }

  @Parameterized.Parameter
  public PageStoreType mPageStoreType;

  private PageStoreOptions mOptions;

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  private PageStoreDir mPageStoreDir;

  @Before
  public void before() throws Exception {
    CacheManagerOptions cacheManagerOptions = CacheManagerOptions.create(mConf);
    mOptions = cacheManagerOptions.getPageStoreOptions().get(0);
    mOptions.setStoreType(mPageStoreType);
    mOptions.setPageSize(PAGE_SIZE);
    mOptions.setCacheSize(CACHE_CAPACITY);
    mOptions.setAlluxioVersion(ProjectConstants.VERSION);
    mOptions.setRootDir(Paths.get(mTemp.getRoot().getAbsolutePath()));

    mPageStoreDir =
        PageStoreDir.createPageStoreDir(cacheManagerOptions.getCacheEvictorOptions(), mOptions);
  }

  @After
  public void after() throws Exception {
    if (mPageStoreDir != null) {
      mPageStoreDir.close();
    }
  }

  @Test
  public void getPages() throws Exception {
    int len = 32;
    int count = 16;
    byte[] data = BufferUtils.getIncreasingByteArray(len);
    Set<PageInfo> pages = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      PageId id = new PageId("0", i);
      mPageStoreDir.getPageStore().put(id, data);
      pages.add(new PageInfo(id, data.length, mPageStoreDir));
    }
    Set<PageInfo> restored = new HashSet<>();
    mPageStoreDir.scanPages((pageInfo -> restored.add(pageInfo.get())));
    if (mOptions.getType().equals(PageStoreType.MEM)) {
      assertTrue(restored.isEmpty());
    } else {
      assertEquals(pages, restored);
    }
  }

  @Test
  public void getPagesUUID() throws Exception {
    int len = 32;
    int count = 16;
    byte[] data = BufferUtils.getIncreasingByteArray(len);
    Set<PageInfo> pages = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      PageId id = new PageId(UUID.randomUUID().toString(), i);
      mPageStoreDir.getPageStore().put(id, data);
      pages.add(new PageInfo(id, data.length, mPageStoreDir));
    }
    Set<PageInfo> restored = new HashSet<>();
    mPageStoreDir.scanPages((pageInfo -> restored.add(pageInfo.get())));
    if (mOptions.getType().equals(PageStoreType.MEM)) {
      assertTrue(restored.isEmpty());
    } else {
      assertEquals(pages, restored);
    }
  }

  @Test
  public void cacheUsage() throws Exception {
    int len = 32;
    int count = 16;
    byte[] data = BufferUtils.getIncreasingByteArray(len);
    for (int i = 0; i < count; i++) {
      PageId id = new PageId("0", i);
      mPageStoreDir.getPageStore().put(id, data);
      mPageStoreDir.putPage(new PageInfo(id, data.length, mPageStoreDir));
    }
    Optional<CacheUsage> usage = mPageStoreDir.getUsage();
    assertEquals(Optional.of(mPageStoreDir.getCapacityBytes()),
        usage.map(CacheUsageView::capacity));
    assertEquals(Optional.of((long) len * count), usage.map(CacheUsageView::used));
    assertEquals(Optional.of(mPageStoreDir.getCapacityBytes() - (long) len * count),
        usage.map(CacheUsageView::available));
    // cache dir currently does not support get file level usage stat
    Optional<CacheUsage> fileUsage = mPageStoreDir.getUsage()
        .flatMap(usage1 -> usage1.partitionedBy(file("0")));
    assertEquals(Optional.empty(), fileUsage);
  }
}
