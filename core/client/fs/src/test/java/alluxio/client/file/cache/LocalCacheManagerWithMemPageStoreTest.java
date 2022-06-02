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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.evictor.UnevictableCacheEvictor;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for the {@link LocalCacheManager} class.
 */
public final class LocalCacheManagerWithMemPageStoreTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int CACHE_SIZE_BYTES = 512 * Constants.KB;
  private static final PageId PAGE_ID1 = new PageId("0L", 0L);
  private static final PageId PAGE_ID2 = new PageId("1L", 1L);
  private static final byte[] PAGE1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);
  private static final byte[] PAGE2 = BufferUtils.getIncreasingByteArray(255, PAGE_SIZE_BYTES);

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf = Configuration.copyGlobal();
  private MetaStore mMetaStore;
  private PageStoreDir mPageStoreDir;
  private CacheEvictor mEvictor;
  private PageStoreOptions mPageStoreOptions;
  private byte[] mBuf = new byte[PAGE_SIZE_BYTES];

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, 0);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.MEM);
    mPageStoreOptions = PageStoreOptions.create(mConf).get(0);
    mPageStoreDir = PageStoreDir.createPageStoreDir(mConf, mPageStoreOptions);
    mEvictor = new FIFOCacheEvictor(mConf);
    mMetaStore = new DefaultMetaStore(mEvictor);
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
    mPageStoreOptions = PageStoreOptions.create(mConf).get(0);
    mPageStoreDir = PageStoreDir.createPageStoreDir(mConf, mPageStoreOptions);
    return createLocalCacheManager(mConf, mMetaStore, ImmutableList.of(mPageStoreDir));
  }

  /**
   * Creates a manager and waits until it is ready.
   */
  private LocalCacheManager createLocalCacheManager(AlluxioConfiguration conf, MetaStore metaStore,
      List<PageStoreDir> dirs) throws Exception {
    LocalCacheManager cacheManager = LocalCacheManager.create(conf, metaStore, dirs);
    CommonUtils.waitFor("restore completed",
        () -> cacheManager.state() == CacheManager.State.READ_WRITE,
        WaitForOptions.defaults().setTimeoutMs(10000));
    return cacheManager;
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
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
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
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
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
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
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
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
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
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
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
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
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
    mMetaStore = new DefaultMetaStore(mEvictor);
    mCacheManager = createLocalCacheManager();
    long numPages = mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE) / PAGE_SIZE_BYTES;
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
    mMetaStore = new DefaultMetaStore(mConf);
    mCacheManager = createLocalCacheManager(mConf, mMetaStore, ImmutableList.of(mPageStoreDir));
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
    mMetaStore = new QuotaMetaStore(mConf);
    mCacheManager = createLocalCacheManager(mConf, mMetaStore, ImmutableList.of(mPageStoreDir));
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
      mMetaStore = new QuotaMetaStore(mConf);
      mPageStoreDir = PageStoreDir.createPageStoreDir(mConf, mPageStoreOptions);
      mCacheManager = createLocalCacheManager(mConf, mMetaStore, ImmutableList.of(mPageStoreDir));
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
      mMetaStore = new QuotaMetaStore(mConf);
      mPageStoreDir = PageStoreDir.createPageStoreDir(mConf, mPageStoreOptions);
      mCacheManager = createLocalCacheManager(mConf, mMetaStore, ImmutableList.of(mPageStoreDir));
      CacheQuota quota = new CacheQuota(ImmutableMap.of(cacheScope.level(),
          (long) CACHE_SIZE_BYTES + 1));
      int cacheSize = CACHE_SIZE_BYTES / PAGE_SIZE_BYTES;
      for (int i = 0; i < 2 * cacheSize; i++) {
        PageId pageId = new PageId("3", i);
        CacheContext context = CacheContext.defaults().setCacheScope(partitionCacheScope)
            .setCacheQuota(quota);
        assertTrue(mCacheManager.put(pageId, page(i, PAGE_SIZE_BYTES), context));
        if (i >= cacheSize) {
          assertEquals(
              0, mCacheManager.get(new PageId("3", i - cacheSize), PAGE_SIZE_BYTES, mBuf, 0));
          //check the subsequent page is still in cache
          assertEquals(true, mMetaStore.hasPage(new PageId("3", i - cacheSize + 1)));
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
      mMetaStore = new QuotaMetaStore(mConf);
      mPageStoreDir = PageStoreDir.createPageStoreDir(mConf, mPageStoreOptions);
      mCacheManager = createLocalCacheManager(mConf, mMetaStore, ImmutableList.of(mPageStoreDir));
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
  public void highStorageOverheadPut() throws Exception {
    // a store that is so inefficient to store any data
    double highOverhead = CACHE_SIZE_BYTES / PAGE_SIZE_BYTES + 0.1;
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, highOverhead);
    mCacheManager = createLocalCacheManager();
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
  }
}
