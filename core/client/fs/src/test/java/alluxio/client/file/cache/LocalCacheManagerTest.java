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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.evictor.LRUCacheEvictor;
import alluxio.client.file.cache.evictor.UnevictableCacheEvictor;
import alluxio.client.file.cache.store.LocalPageStore;
import alluxio.client.file.cache.store.LocalPageStoreDir;
import alluxio.client.file.cache.store.LocalPageStoreOptions;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Tests for the {@link LocalCacheManager} class.
 */
public final class LocalCacheManagerTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int CACHE_SIZE_BYTES = 512 * Constants.KB;
  private static final PageId PAGE_ID1 = new PageId("0L", 0L);
  private static final PageId PAGE_ID2 = new PageId("1L", 1L);
  private static final byte[] PAGE1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);
  private static final byte[] PAGE2 = BufferUtils.getIncreasingByteArray(255, PAGE_SIZE_BYTES);

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf = Configuration.copyGlobal();
  private PageMetaStore mPageMetaStore;
  private CacheEvictor mEvictor;
  private LocalPageStoreOptions mPageStoreOptions;
  private byte[] mBuf = new byte[PAGE_SIZE_BYTES];

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();
  private PageStore mPageStore;
  private LocalPageStoreDir mPageStoreDir;

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(CACHE_SIZE_BYTES));
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIRS, mTemp.getRoot().getAbsolutePath());
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, 0);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.LOCAL);
    // default setting in prestodb
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, true);
    // default setting in prestodb
    mConf.set(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION, "60s");
    mPageStoreOptions = (LocalPageStoreOptions) PageStoreOptions.create(mConf).get(0);
    mPageStore = PageStore.create(mPageStoreOptions);
    mEvictor = new FIFOCacheEvictor(mConf);
    PageStoreDir.clear(mPageStoreOptions.getRootDir());
    mPageStoreDir = new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor);
    mPageMetaStore = new DefaultPageMetaStore();
    mCacheManager = createLocalCacheManager();
  }

  private byte[] page(int i, int pageLen) {
    return BufferUtils.getIncreasingByteArray(i, pageLen);
  }

  private PageId pageId(long i, int pageIndex) {
    return new PageId(Long.toString(i), pageIndex);
  }

  /**
   * Creates a manager and waits until it is ready.
   */
  private LocalCacheManager createLocalCacheManager() throws Exception {
    mPageStoreOptions = (LocalPageStoreOptions) PageStoreOptions.create(mConf).get(0);
    mPageStore = PageStore.create(mPageStoreOptions);
    mPageStoreDir = new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor);
    mEvictor = new FIFOCacheEvictor(mConf);
    return createLocalCacheManager(mConf, mPageMetaStore, mPageStoreDir);
  }

  /**
   * Creates a manager and waits until it is ready.
   */
  private LocalCacheManager createLocalCacheManager(AlluxioConfiguration conf,
      PageMetaStore pageMetaStore,
      PageStoreDir dir) throws Exception {
    LocalCacheManager cacheManager =
        LocalCacheManager.create(conf, pageMetaStore, ImmutableList.of(dir));
    CommonUtils.waitFor("restore completed",
        () -> cacheManager.state() == CacheManager.State.READ_WRITE,
        WaitForOptions.defaults().setTimeoutMs(10000));
    return cacheManager;
  }

  @Test
  public void createNonexistentRootDirSyncRestore() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIRS,
        PathUtils.concatPath(mTemp.getRoot().getAbsolutePath(), UUID.randomUUID().toString()));
    assertNotNull(
        LocalCacheManager.create(mConf, mPageMetaStore, PageStoreDir.createPageStoreDirs(mConf)));
  }

  @Test
  public void createNonexistentRootDirAsyncRestore() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, true);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIRS,
        PathUtils.concatPath(mTemp.getRoot().getAbsolutePath(), UUID.randomUUID().toString()));
    assertNotNull(
        LocalCacheManager.create(mConf, mPageMetaStore, PageStoreDir.createPageStoreDirs(mConf)));
  }

  @Test
  public void createUnwriableRootDirSyncRestore() throws Exception {
    File root = mTemp.newFolder();
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIRS, root.getAbsolutePath());
    try {
      root.setWritable(false);
      LocalCacheManager.create(mConf, mPageMetaStore, PageStoreDir.createPageStoreDirs(mConf));
      fail();
    } catch (Exception e) {
      // expected
    } finally {
      root.setWritable(true);
    }
  }

  @Test
  public void createUnwriableRootDirAsyncRestore() throws Exception {
    File root = mTemp.newFolder();
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, true);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIRS, root.getAbsolutePath());
    try {
      root.setWritable(false);
      mCacheManager =
          LocalCacheManager.create(mConf, mPageMetaStore, PageStoreDir.createPageStoreDirs(mConf));
      CommonUtils.waitFor("async restore completed",
          () -> mCacheManager.state() == CacheManager.State.NOT_IN_USE,
          WaitForOptions.defaults().setTimeoutMs(10000));
    } finally {
      root.setWritable(true);
    }
  }

  @Test
  public void putNew() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
  }

  @Test
  public void putExist() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE2));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
  }

  @Test
  public void putEvict() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE_SIZE_BYTES));
    mCacheManager = createLocalCacheManager();
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2));
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
  }

  @Test
  public void putSmallPages() throws Exception {
    // Cache size is only one full page, but should be able to store multiple small pages
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE_SIZE_BYTES));
    mCacheManager = createLocalCacheManager();
    int smallPageLen = 8;
    long numPages = mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE) / smallPageLen;
    for (int i = 0; i < numPages; i++) {
      PageId id = pageId(i, 0);
      byte[] smallPage = page(i, smallPageLen);
      assertTrue(mCacheManager.put(id, smallPage));
    }
    for (int i = 0; i < numPages; i++) {
      PageId id = pageId(i, 0);
      byte[] buf = new byte[smallPageLen];
      assertEquals(smallPageLen, mCacheManager.get(id, smallPageLen, buf, 0));
      assertArrayEquals(page(i, smallPageLen), buf);
    }
  }

  @Test
  public void evictSmallPageByPutSmallPage() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE_SIZE_BYTES));
    mCacheManager = createLocalCacheManager();
    int smallPageLen = 8;
    long numPages = mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE) / smallPageLen;
    for (int i = 0; i < numPages; i++) {
      PageId id = pageId(i, 0);
      assertTrue(mCacheManager.put(id, page(i, smallPageLen)));
    }
    // this should trigger evicting the first page (by FIFO)
    assertTrue(mCacheManager.put(pageId(numPages, 0), page(-1, smallPageLen)));
    for (int i = 0; i < numPages; i++) {
      byte[] buf = new byte[smallPageLen];
      PageId id = pageId(i, 0);
      if (i == 0) {
        assertEquals(0, mCacheManager.get(id, smallPageLen, buf, 0));
      } else {
        assertEquals(smallPageLen, mCacheManager.get(id, smallPageLen, buf, 0));
        assertArrayEquals(page(i, smallPageLen), buf);
      }
    }
  }

  @Test
  public void evictSmallPagesByPutPigPageWithoutRetry() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE_SIZE_BYTES));
    mConf.set(PropertyKey.USER_CLIENT_CACHE_EVICTION_RETRIES, 0);
    mCacheManager = createLocalCacheManager();
    int smallPageLen = 8;
    long numPages = mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE) / smallPageLen;
    for (int i = 0; i < numPages; i++) {
      PageId id = pageId(i, 0);
      assertTrue(mCacheManager.put(id, page(i, smallPageLen)));
    }
    byte[] bigPage = page(0, PAGE_SIZE_BYTES);
    PageId bigPageId = pageId(-1, 0);
    for (int i = 0; i < numPages - 1; i++) {
      assertFalse(mCacheManager.put(bigPageId, bigPage));
    }
    assertTrue(mCacheManager.put(bigPageId, bigPage));
    byte[] buf = new byte[PAGE_SIZE_BYTES];
    assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(bigPageId, PAGE_SIZE_BYTES, buf, 0));
    assertArrayEquals(bigPage, buf);
  }

  @Test
  public void evictSmallPagesByPutPigPageWithRetry() throws Exception {
    int smallPageLen = 8;
    int numPages = (int) (mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE) / smallPageLen);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE_SIZE_BYTES));
    mConf.set(PropertyKey.USER_CLIENT_CACHE_EVICTION_RETRIES, numPages);
    mCacheManager = createLocalCacheManager();
    for (int i = 0; i < numPages; i++) {
      PageId id = pageId(i, 0);
      assertTrue(mCacheManager.put(id, page(i, smallPageLen)));
    }
    byte[] bigPage = page(0, PAGE_SIZE_BYTES);
    PageId bigPageId = pageId(-1, 0);
    assertTrue(mCacheManager.put(bigPageId, bigPage));
    byte[] buf = new byte[PAGE_SIZE_BYTES];
    assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(bigPageId, PAGE_SIZE_BYTES, buf, 0));
    assertArrayEquals(bigPage, buf);
  }

  @Test
  public void evictBigPagesByPutSmallPage() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE_SIZE_BYTES));
    mCacheManager = createLocalCacheManager();
    PageId bigPageId = pageId(-1, 0);
    assertTrue(mCacheManager.put(bigPageId, page(0, PAGE_SIZE_BYTES)));
    int smallPageLen = 8;
    long numPages = mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE) / smallPageLen;
    byte[] smallPage = BufferUtils.getIncreasingByteArray(smallPageLen);
    for (int i = 0; i < numPages; i++) {
      PageId id = pageId(i, 0);
      assertTrue(mCacheManager.put(id, smallPage));
    }
  }

  @Test
  public void noEvictionPolicy() throws Exception {
    mEvictor = new UnevictableCacheEvictor(mConf);
    mPageMetaStore = new DefaultPageMetaStore();
    mCacheManager = createLocalCacheManager();
    long numPages = mPageStoreOptions.getCacheSize() / PAGE_SIZE_BYTES;
    for (int i = 0; i < numPages; i++) {
      PageId id = pageId(i, 0);
      assertTrue(mCacheManager.put(id, PAGE1));
    }
    assertFalse(mCacheManager.put(pageId(numPages, 0), PAGE1));
  }

  @Test
  public void putGetPartialPage() throws Exception {
    int[] sizeArray = {1, PAGE_SIZE_BYTES / 2, PAGE_SIZE_BYTES - 1};
    for (int size : sizeArray) {
      PageId pageId = new PageId("3", size);
      byte[] pageData = page(0, size);
      assertTrue(mCacheManager.put(pageId, pageData));
      byte[] buf = new byte[size];
      assertEquals(size, mCacheManager.get(pageId, size, buf, 0));
      assertArrayEquals(pageData, buf);
    }
  }

  @Test
  public void putMoreThanCacheCapacityFIFO() throws Exception {
    int cacheSize = CACHE_SIZE_BYTES / PAGE_SIZE_BYTES;
    for (int i = 0; i < 2 * cacheSize; i++) {
      PageId pageId = new PageId("3", i);
      mCacheManager.put(pageId, page(i, PAGE_SIZE_BYTES));
      if (i >= cacheSize) {
        PageId id = new PageId("3", i - cacheSize + 1);
        assertEquals(0,
            mCacheManager.get(new PageId("3", i - cacheSize), PAGE_SIZE_BYTES, mBuf, 0));
        assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(id, PAGE_SIZE_BYTES, mBuf, 0));
        assertArrayEquals(page(i - cacheSize + 1, PAGE_SIZE_BYTES), mBuf);
      }
    }
  }

  @Test
  public void putMoreThanCacheCapacityLRU() throws Exception {
    mPageMetaStore = new DefaultPageMetaStore();
    mEvictor = new LRUCacheEvictor(mConf);
    mPageStoreDir = new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, mPageStoreDir);
    int cacheSize = CACHE_SIZE_BYTES / PAGE_SIZE_BYTES;
    //fill up the cache
    for (int i = 0; i < cacheSize; i++) {
      PageId pageId = new PageId("3", i);
      mCacheManager.put(pageId, page(i, PAGE_SIZE_BYTES));
    }
    //define page index 2,3,5 as active pages
    List<PageId> activePageIds = ImmutableList.of(2, 3, 5).stream()
        .map(pageIndex -> new PageId("3", pageIndex)).collect(
            Collectors.toList());
    //get active pages 2,3,5 to update LRU
    activePageIds.forEach(pageId -> mCacheManager.get(pageId, PAGE_SIZE_BYTES, mBuf, 0));
    //Partially fill up the cache again with new pages
    for (int i = cacheSize; i < 2 * cacheSize - 3; i++) {
      PageId pageId = new PageId("3", i);
      mCacheManager.put(pageId, page(i, PAGE_SIZE_BYTES));
    }
    //check page 2,3,5 is still in cache
    activePageIds.forEach(pageId -> {
      assertEquals(PAGE_SIZE_BYTES,
          mCacheManager.get(pageId, PAGE_SIZE_BYTES, mBuf, 0));
      assertArrayEquals(page((int) pageId.getPageIndex(), PAGE_SIZE_BYTES), mBuf);
    });
    //check the pages other than 2,3,5 got evicted
    for (int i = 0; i < cacheSize; i++) {
      PageId pageId = new PageId("3", i);
      if (!activePageIds.contains(pageId)) {
        assertEquals(0,
            mCacheManager.get(pageId, PAGE_SIZE_BYTES, mBuf, 0));
      }
    }
  }

  @Test
  public void putWithInsufficientQuota() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, true);
    mPageMetaStore = new QuotaPageMetaStore(mConf);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, mPageStoreDir);
    CacheScope scope = CacheScope.create("schema.table.partition");

    CacheContext context = CacheContext.defaults().setCacheScope(scope);
    // insufficient partition quota
    assertFalse(mCacheManager
        .put(PAGE_ID1, PAGE1, context.setCacheQuota(new CacheQuota(
            ImmutableMap.of(CacheScope.Level.PARTITION, (long) PAGE1.length - 1)))));
    // insufficient table quota
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1, context.setCacheQuota(new CacheQuota(
        ImmutableMap.of(CacheScope.Level.TABLE, (long) PAGE1.length - 1)))));
    // insufficient schema quota
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1, context.setCacheQuota(new CacheQuota(
        ImmutableMap.of(CacheScope.Level.SCHEMA, (long) PAGE1.length - 1)))));
    // insufficient global quota
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1, context.setCacheQuota(new CacheQuota(
        ImmutableMap.of(CacheScope.Level.GLOBAL, (long) PAGE1.length - 1)))));
    // without quota
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
  }

  @Test
  public void putWithQuotaEviction() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, true);
    CacheScope partitionCacheScope = CacheScope.create("schema.table.partition");
    CacheScope tableCacheScope = CacheScope.create("schema.table");
    CacheScope schemaCacheScope = CacheScope.create("schema");

    CacheScope[] quotaCacheScopes =
        {partitionCacheScope, tableCacheScope, schemaCacheScope, CacheScope.GLOBAL};
    for (CacheScope cacheScope : quotaCacheScopes) {
      mPageMetaStore = new QuotaPageMetaStore(mConf);
      PageStoreDir.clear(mPageStoreOptions.getRootDir());
      mPageStore = PageStore.create(mPageStoreOptions);
      mEvictor = CacheEvictor.create(mConf);
      mPageStoreDir = new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor);
      mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, mPageStoreDir);
      CacheQuota quota =
          new CacheQuota(ImmutableMap.of(cacheScope.level(),
              (long) PAGE1.length + PAGE2.length - 1));
      CacheContext context = CacheContext.defaults().setCacheScope(partitionCacheScope)
          .setCacheQuota(quota);
      assertTrue(mCacheManager.put(PAGE_ID1, PAGE1, context));
      assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
      assertTrue(mCacheManager.put(PAGE_ID2, PAGE2, context));
      assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
      assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    }
  }

  @Test
  public void putWithQuotaMoreThanCacheCapacity() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, true);
    CacheScope partitionCacheScope = CacheScope.create("schema.table.partition");
    CacheScope tableCacheScope = CacheScope.create("schema.table");
    CacheScope schemaCacheScope = CacheScope.create("schema");

    CacheScope[] quotaCacheScopes =
        {partitionCacheScope, tableCacheScope, schemaCacheScope, CacheScope.GLOBAL};
    for (CacheScope cacheScope : quotaCacheScopes) {
      mPageMetaStore = new QuotaPageMetaStore(mConf);
      mPageStoreDir.reset();
      mPageStore = PageStore.create(mPageStoreOptions);
      mEvictor = CacheEvictor.create(mConf);
      mPageStoreDir = new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor);
      mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, mPageStoreDir);
      CacheQuota quota = new CacheQuota(ImmutableMap.of(cacheScope.level(),
          (long) CACHE_SIZE_BYTES + 1));
      int cacheSize = CACHE_SIZE_BYTES / PAGE_SIZE_BYTES;
      for (int i = 0; i < 2 * cacheSize; i++) {
        PageId pageId = new PageId("3", i);
        CacheContext context = CacheContext.defaults().setCacheScope(partitionCacheScope)
            .setCacheQuota(quota);
        assertTrue(String.valueOf(i), mCacheManager.put(pageId, page(i, PAGE_SIZE_BYTES), context));
        if (i >= cacheSize) {
          assertEquals(
              0, mCacheManager.get(new PageId("3", i - cacheSize), PAGE_SIZE_BYTES, mBuf, 0));
          //check the subsequent page is still in cache
          assertEquals(true, mPageMetaStore.hasPage(new PageId("3", i - cacheSize + 1)));
        }
      }
    }
  }

  @Test
  public void putWithInsufficientParentQuota() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, true);
    CacheScope partitionCacheScope1 = CacheScope.create("schema.table.partition1");
    CacheScope partitionCacheScope2 = CacheScope.create("schema.table.partition2");
    CacheScope tableCacheScope = CacheScope.create("schema.table");
    CacheScope schemaCacheScope = CacheScope.create("schema");
    CacheScope[] quotaCacheScopes = {tableCacheScope, schemaCacheScope, CacheScope.GLOBAL};
    for (CacheScope cacheScope : quotaCacheScopes) {
      mPageMetaStore = new QuotaPageMetaStore(mConf);
      PageStoreDir.clear(mPageStoreOptions.getRootDir());
      mPageStore = PageStore.create(mPageStoreOptions);
      mEvictor = CacheEvictor.create(mConf);
      mPageStoreDir = new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor);
      mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, mPageStoreDir);
      CacheQuota quota = new CacheQuota(ImmutableMap.of(
          partitionCacheScope1.level(), (long) PAGE1.length + PAGE2.length,
          cacheScope.level(), (long) PAGE1.length + PAGE2.length - 1
      ));
      CacheContext context1 = CacheContext.defaults().setCacheScope(partitionCacheScope1)
          .setCacheQuota(quota);
      assertTrue(mCacheManager.put(PAGE_ID1, PAGE1, context1));
      assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
      CacheContext context2 = CacheContext.defaults().setCacheScope(partitionCacheScope2)
          .setCacheQuota(quota);
      assertTrue(mCacheManager.put(PAGE_ID2, PAGE2, context2));
      assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
      assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    }
  }

  @Test
  public void putLargeId() throws Exception {
    long[] fileIdArray = {Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE + 1};
    long[] pageIndexArray = {Long.MAX_VALUE - 1, Long.MAX_VALUE};
    for (long fileId : fileIdArray) {
      for (long pageIndexId : pageIndexArray) {
        PageId largeId = new PageId("0", pageIndexId);
        mCacheManager.put(largeId, PAGE1);
        assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(largeId, PAGE1.length, mBuf, 0));
        assertArrayEquals(PAGE1, mBuf);
      }
    }
  }

  @Test
  public void getExist() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1);
    assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
  }

  @Test
  public void getNotExist() throws Exception {
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
  }

  @Test
  public void getOffset() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1);
    int[] offsetArray = {0, 1, PAGE_SIZE_BYTES / 2, PAGE_SIZE_BYTES - 1};
    for (int offset : offsetArray) {
      assertEquals(PAGE1.length - offset,
          mCacheManager.get(PAGE_ID1, offset, PAGE1.length - offset, mBuf, 0));
      assertEquals(ByteBuffer.wrap(PAGE1, offset, PAGE1.length - offset),
          ByteBuffer.wrap(mBuf, 0, PAGE1.length - offset));
    }
  }

  @Test
  public void getNotEnoughSpaceException() throws Exception {
    byte[] buf = new byte[PAGE1.length - 1];
    assertThrows(IllegalArgumentException.class, () ->
        mCacheManager.get(PAGE_ID1, PAGE1.length, buf, 0));
  }

  @Test
  public void deleteExist() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1);
    mCacheManager.delete(PAGE_ID1);
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
  }

  @Test
  public void deleteNotExist() throws Exception {
    assertFalse(mCacheManager.delete(PAGE_ID1));
  }

  @Test
  public void syncRestore() throws Exception {
    mCacheManager.close();
    List<PageStoreDir> dirs = PageStoreDir.createPageStoreDirs(mConf);
    PageStore pageStore = dirs.get(0).getPageStore(); // previous page store has been closed
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    pageStore.put(PAGE_ID1, PAGE1);
    pageStore.put(pageUuid, PAGE2);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, false);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, dirs.get(0));
    assertEquals(CacheManager.State.READ_WRITE, mCacheManager.state());
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    assertEquals(PAGE2.length, mCacheManager.get(pageUuid, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
  }

  @Test
  public void asyncRestore() throws Exception {
    mCacheManager.close();
    mPageStore = PageStore.create(mPageStoreOptions); // previous page store has been closed
    mPageStore.put(PAGE_ID1, PAGE1);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore,
        new LocalPageStoreDir(mPageStoreOptions, mPageStore, mEvictor));
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
  }

  @Test
  public void asyncRestoreReadOnly() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, true);
    mCacheManager.close();
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    SlowGetPageStoreDir slowGetPageStoreDir = new SlowGetPageStoreDir(
        mPageStoreOptions,
        new LocalPageStoreDir(mPageStoreOptions, new LocalPageStore(mPageStoreOptions), mEvictor));
    slowGetPageStoreDir.getPageStore().put(PAGE_ID1, PAGE1);
    slowGetPageStoreDir.getPageStore().put(pageUuid, PAGE2);
    mCacheManager = LocalCacheManager.create(
        mConf, mPageMetaStore, ImmutableList.of(slowGetPageStoreDir));
    assertEquals(CacheManager.State.READ_ONLY, mCacheManager.state());
    Thread.sleep(1000); // some buffer to restore page1
    // In READ_ONLY mode we still get previously added page
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    assertEquals(PAGE2.length, mCacheManager.get(pageUuid, PAGE2.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
    // In READ_ONLY mode we cannot put
    assertFalse(mCacheManager.put(PAGE_ID2, PAGE2));
    // In READ_ONLY mode we cannot delete
    assertFalse(mCacheManager.delete(PAGE_ID1));
    // stop the "fake scan"
    slowGetPageStoreDir.mScanComplete.set(true);
    CommonUtils.waitFor("async restore completed",
        () -> mCacheManager.state() == CacheManager.State.READ_WRITE,
        WaitForOptions.defaults().setTimeoutMs(10000));
    // Put get back to normal
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2));
    assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
    assertTrue(mCacheManager.delete(PAGE_ID2));
  }

  @Test
  public void syncRestoreUnknownFile() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, false);
    mCacheManager.close();
    PageStoreDir dir =
        PageStoreDir.createPageStoreDirs(mConf).get(0); // previous page store has been closed
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    dir.getPageStore().put(PAGE_ID1, PAGE1);
    dir.getPageStore().put(pageUuid, PAGE2);
    String rootDir = mPageStoreOptions.getRootDir().toString();
    FileUtils.createFile(Paths.get(rootDir, "invalidPageFile").toString());
    mCacheManager = LocalCacheManager.create(mConf, mPageMetaStore, ImmutableList.of(dir));
    assertEquals(CacheManager.State.READ_WRITE, mCacheManager.state());
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertEquals(0, mCacheManager.get(pageUuid, PAGE2.length, mBuf, 0));
  }

  @Test
  public void asyncRestoreUnknownFile() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, true);
    mCacheManager.close();
    PageStoreDir dir =
        PageStoreDir.createPageStoreDirs(mConf).get(0); // previous page store has been closed
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    dir.getPageStore().put(PAGE_ID1, PAGE1);
    dir.getPageStore().put(pageUuid, PAGE2);
    String rootDir = mPageStoreOptions.getRootDir().toString();
    FileUtils.createFile(Paths.get(rootDir, "invalidPageFile").toString());
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, dir);
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertEquals(0, mCacheManager.get(pageUuid, PAGE2.length, mBuf, 0));
  }

  @Test
  public void syncRestoreUnwritableRootDir() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, false);
    mCacheManager.close();
    PageStoreDir dir =
        PageStoreDir.createPageStoreDirs(mConf).get(0); // previous page store has been closed
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    dir.getPageStore().put(PAGE_ID1, PAGE1);
    dir.getPageStore().put(pageUuid, PAGE2);
    String rootDir = mPageStoreOptions.getRootDir().toString();
    FileUtils.deletePathRecursively(rootDir);
    File rootParent = new File(rootDir).getParentFile();
    try {
      rootParent.setWritable(false);
      LocalCacheManager.create(mConf, mPageMetaStore, ImmutableList.of(dir));
    } catch (Exception e) {
      // expected case
    } finally {
      rootParent.setWritable(true);
    }
  }

  @Test
  public void asyncRestoreUnwritableRootDir() throws Exception {
    mCacheManager.close();
    PageStoreDir dir =
        PageStoreDir.createPageStoreDirs(mConf).get(0); // previous page store has been closed
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    dir.getPageStore().put(PAGE_ID1, PAGE1);
    dir.getPageStore().put(pageUuid, PAGE2);
    String rootDir = mPageStoreOptions.getRootDir().toString();
    FileUtils.deletePathRecursively(rootDir);
    File rootParent = new File(rootDir).getParentFile();
    rootParent.setWritable(false);
    try {
      mCacheManager = LocalCacheManager.create(mConf, mPageMetaStore, ImmutableList.of(dir));
      CommonUtils.waitFor("async restore completed",
          () -> mCacheManager.state() == CacheManager.State.NOT_IN_USE,
          WaitForOptions.defaults().setTimeoutMs(10000));
      assertEquals(-1, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
      assertFalse(mCacheManager.put(PAGE_ID2, PAGE2));
      assertFalse(mCacheManager.delete(PAGE_ID1));
    } finally {
      rootParent.setWritable(true);
    }
  }

  @Test
  public void syncRestoreWithMorePagesThanCapacity() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE1.length + PAGE2.length));
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, false);
    mCacheManager.close();
    PageStoreDir dir =
        PageStoreDir.createPageStoreDirs(mConf).get(0); // previous page store has been closed
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    dir.getPageStore().put(PAGE_ID1, PAGE1);
    dir.getPageStore().put(PAGE_ID2, PAGE2);
    dir.getPageStore().put(pageUuid, BufferUtils.getIncreasingByteArray(
        PAGE1.length + PAGE2.length + 1));
    dir = PageStoreDir.createPageStoreDirs(mConf).get(0);
    mCacheManager = LocalCacheManager.create(mConf, mPageMetaStore, ImmutableList.of(dir));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
    assertEquals(0, mCacheManager.get(pageUuid, PAGE2.length, mBuf, 0));
  }

  @Test
  public void asyncRestoreWithMorePagesThanCapacity() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE1.length + PAGE2.length));
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, true);
    mCacheManager.close();
    PageStoreDir dir =
        PageStoreDir.createPageStoreDirs(mConf).get(0); // previous page store has been closed
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    dir.getPageStore().put(PAGE_ID1, PAGE1);
    dir.getPageStore().put(PAGE_ID2, PAGE2);
    dir.getPageStore().put(pageUuid, BufferUtils.getIncreasingByteArray(
        PAGE1.length + PAGE2.length + 1));
    dir = PageStoreDir.createPageStoreDirs(mConf).get(0);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, dir);
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
    assertEquals(0, mCacheManager.get(pageUuid, PAGE2.length, mBuf, 0));
  }

  @Test
  public void asyncCache() throws Exception {
    final int threads = 16;
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, true);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_THREADS, threads);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.LOCAL);
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    HangingPageStore pageStore = new HangingPageStore(pageStoreOptions);
    PageStoreDir dir =
        new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions, pageStore, mEvictor);

    pageStore.setPutHanging(true);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, dir);
    for (int i = 0; i < threads; i++) {
      PageId pageId = new PageId("5", i);
      assertTrue(mCacheManager.put(pageId, page(i, PAGE_SIZE_BYTES)));
    }
    // fail due to full queue
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
    pageStore.setPutHanging(false);
    while (pageStore.getPuts() < threads) {
      Thread.sleep(1000);
    }
    pageStore.setPutHanging(true);
    for (int i = 0; i < threads; i++) {
      PageId pageId = new PageId("6", i);
      assertTrue(mCacheManager.put(pageId, page(i, PAGE_SIZE_BYTES)));
    }
  }

  @Test
  public void asyncCacheSamePage() throws Exception {
    final int threads = 16;
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, true);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_THREADS, threads);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.LOCAL);
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    HangingPageStore pageStore = new HangingPageStore(pageStoreOptions);
    PageStoreDir dir =
        new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions, pageStore, mEvictor);
    pageStore.setPutHanging(true);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, dir);
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
    pageStore.setPutHanging(false);
    while (pageStore.getPuts() < 1) {
      Thread.sleep(1000);
    }
    pageStore.setPutHanging(true);
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
  }

  @Test
  public void recoverCacheFromFailedPut() throws Exception {
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    FaultyPageStore pageStore = new FaultyPageStore();
    PageStoreDir dir =
        new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions, pageStore, mEvictor);

    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, dir);
    pageStore.setPutFaulty(true);
    // a failed put
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
    // no state left after previous failed put
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    // can ask to put same page again without exception
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
    // still no state left
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    pageStore.setPutFaulty(false);
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
  }

  @Test
  public void failedPageStoreDeleteOnEviction() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE_SIZE_BYTES));
    FaultyPageStore pageStore = new FaultyPageStore();
    LocalPageStoreDir dir = new LocalPageStoreDir(mPageStoreOptions, pageStore, mEvictor);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, dir);
    // first put should be ok
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    // trigger a failed eviction
    pageStore.setPutFaulty(true);
    // put operation failed because we do not retry for the IO exception from put
    assertFalse(mCacheManager.put(PAGE_ID2, PAGE2));
    //clear put faulty and set delete faulty to true
    pageStore.setPutFaulty(false);
    pageStore.setDeleteFaulty(true);
    // put operation succeeded after retry
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2));
    assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
    // restore page store to function
    pageStore.setDeleteFaulty(false);
    // trigger another eviction, this should work
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
  }

  @Test
  public void putTimeout() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION, "2s");
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(PAGE_SIZE_BYTES));
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    HangingPageStore pageStore = new HangingPageStore(pageStoreOptions);
    PageStoreDir dir = new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions,
        new TimeBoundPageStore(pageStore, pageStoreOptions), mEvictor);
    LocalCacheManager cacheManager =
        createLocalCacheManager(mConf, mPageMetaStore, dir);
    assertTrue(cacheManager.put(PAGE_ID1, PAGE1));
    pageStore.setPutHanging(true);
    assertFalse(cacheManager.put(PAGE_ID2, PAGE2));
    pageStore.setPutHanging(false);
  }

  @Test
  public void getTimeout() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION, "2s");
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    HangingPageStore pageStore = new HangingPageStore(pageStoreOptions);
    PageStoreDir dir = new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions,
        new TimeBoundPageStore(pageStore, pageStoreOptions), mEvictor);
    LocalCacheManager cacheManager =
        createLocalCacheManager(mConf, mPageMetaStore, dir);
    assertTrue(cacheManager.put(PAGE_ID1, PAGE1));
    pageStore.setGetHanging(true);
    assertEquals(-1, cacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    pageStore.setGetHanging(false);
  }

  @Test
  public void deleteTimeout() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION, "2s");
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    HangingPageStore pageStore = new HangingPageStore(pageStoreOptions);
    PageStoreDir dir = new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions,
        new TimeBoundPageStore(pageStore, pageStoreOptions), mEvictor);
    LocalCacheManager cacheManager =
        createLocalCacheManager(mConf, mPageMetaStore, dir);
    assertTrue(cacheManager.put(PAGE_ID1, PAGE1));
    pageStore.setDeleteHanging(true);
    assertFalse(cacheManager.delete(PAGE_ID1));
    pageStore.setDeleteHanging(false);
  }

  @Test
  public void noSpaceLeftPageStorePut() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(
        (LocalPageStoreOptions) PageStoreOptions.create(mConf).get(0)) {
      private long mFreeBytes = PAGE_SIZE_BYTES;

      @Override
      public void delete(PageId pageId) throws IOException, PageNotFoundException {
        mFreeBytes += PAGE_SIZE_BYTES;
        super.delete(pageId);
      }

      @Override
      public void put(PageId pageId, byte[] page) throws IOException {
        if (mFreeBytes < page.length) {
          throw new ResourceExhaustedException("No space left on device");
        }
        mFreeBytes -= page.length;
        super.put(pageId, page);
      }
    };
    PageStoreDir dir =
        new LocalPageStoreDir((LocalPageStoreOptions) PageStoreOptions.create(mConf).get(0),
            pageStore, mEvictor);
    mCacheManager = createLocalCacheManager(mConf, mPageMetaStore, dir);
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    // trigger evicting PAGE1
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2));
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
  }

  @Test
  public void highStorageOverheadPut() throws Exception {
    // a store that is so inefficient to store any data
    double highOverhead = CACHE_SIZE_BYTES / PAGE_SIZE_BYTES + 0.1;
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, highOverhead);
    mCacheManager = createLocalCacheManager();
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
  }

  @Test
  public void listPageIds() throws Exception {
    mCacheManager = createLocalCacheManager();
    assertEquals(0,
        mCacheManager.getCachedPageIdsByFileId(PAGE_ID1.getFileId(), 64 * PAGE_SIZE_BYTES).size());
    mCacheManager.put(PAGE_ID1, PAGE1);
    assertEquals(PAGE_ID1,
        mCacheManager.getCachedPageIdsByFileId(PAGE_ID1.getFileId(), 64 * PAGE_SIZE_BYTES).get(0));
    PageId pageId5 = new PageId(PAGE_ID1.getFileId(), 5);
    mCacheManager.put(pageId5, PAGE1);
    assertEquals(PAGE_ID1,
        mCacheManager.getCachedPageIdsByFileId(PAGE_ID1.getFileId(), 64 * PAGE_SIZE_BYTES).get(0));
    assertEquals(pageId5,
        mCacheManager.getCachedPageIdsByFileId(PAGE_ID1.getFileId(), 64 * PAGE_SIZE_BYTES).get(1));
    //Store a page smaller than full size
    PageId pageId6 = new PageId(PAGE_ID1.getFileId(), 6);
    mCacheManager.put(pageId6, BufferUtils.getIncreasingByteArray(135));
    assertEquals(pageId6,
        mCacheManager.getCachedPageIdsByFileId(PAGE_ID1.getFileId(),
            6 * PAGE_SIZE_BYTES + 135).get(2));
    //Store a file smaller than one page
    PageId smallFilePageId = new PageId("small_file", 0);
    mCacheManager.put(smallFilePageId, BufferUtils.getIncreasingByteArray(135));
    assertEquals(smallFilePageId,
        mCacheManager.getCachedPageIdsByFileId(smallFilePageId.getFileId(),
            135).get(0));
    //Store a zero length file
    PageId zeroLenFilePageId = new PageId("zero_len_file", 0);
    mCacheManager.put(zeroLenFilePageId, BufferUtils.getIncreasingByteArray(0));
    assertEquals(zeroLenFilePageId,
        mCacheManager.getCachedPageIdsByFileId(zeroLenFilePageId.getFileId(),
            0).get(0));
  }

  /**
   * A PageStore where put can throw IOException on put or delete.
   */
  private class FaultyPageStore extends LocalPageStore {
    public FaultyPageStore() {
      super((LocalPageStoreOptions) PageStoreOptions.create(mConf).get(0));
    }

    private AtomicBoolean mPutFaulty = new AtomicBoolean(false);
    private AtomicBoolean mDeleteFaulty = new AtomicBoolean(false);

    @Override
    public void put(PageId pageId, byte[] page) throws IOException {
      if (mPutFaulty.get()) {
        throw new IOException("Not found");
      }
      super.put(pageId, page);
    }

    @Override
    public void delete(PageId pageId) throws IOException, PageNotFoundException {
      if (mDeleteFaulty.get()) {
        throw new IOException("Not found");
      }
      super.delete(pageId);
    }

    void setPutFaulty(boolean faulty) {
      mPutFaulty.set(faulty);
    }

    void setDeleteFaulty(boolean faulty) {
      mDeleteFaulty.set(faulty);
    }
  }

  /**
   * A PageStore with slow scan.
   */
  private class SlowGetPageStoreDir extends LocalPageStoreDir {

    private final PageStoreDir mUnderPageStoreDir;

    private AtomicInteger mPageId = new AtomicInteger(100);
    private AtomicBoolean mScanComplete = new AtomicBoolean(false);

    public SlowGetPageStoreDir(LocalPageStoreOptions pageStoreOptions,
                               PageStoreDir pageStoreDir) {
      super(pageStoreOptions, pageStoreDir.getPageStore(), mEvictor);
      mUnderPageStoreDir = pageStoreDir;
    }

    @Override
    public void scanPages(Consumer<PageInfo> pageInfoConsumer) throws IOException {
      mUnderPageStoreDir.scanPages(pageInfoConsumer);
      while (!mScanComplete.get()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        PageId id = new PageId(String.valueOf(mPageId.getAndIncrement()), 0L);
        pageInfoConsumer.accept(new PageInfo(id, 1L, this));
      }
    }
  }
}
