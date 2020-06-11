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
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.file.cache.store.LocalPageStore;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.FileUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

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
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private MetaStore mMetaStore;
  private PageStore mPageStore;
  private CacheEvictor mEvictor;
  private byte[] mBuf = new byte[PAGE_SIZE_BYTES];

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_DIR, mTemp.getRoot().getAbsolutePath());
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, false);
    mMetaStore = MetaStore.create();
    mPageStore = PageStore.create(PageStoreOptions.create(mConf), true);
    mEvictor = new FIFOEvictor(mMetaStore);
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, mPageStore, mEvictor);
  }

  private byte[] page(int i, int pageLen) {
    return BufferUtils.getIncreasingByteArray(i, pageLen);
  }

  private PageId pageId(long i, int pageIndex) {
    return new PageId(Long.toString(i), pageIndex);
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
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE2));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
  }

  @Test
  public void putEvict() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
    mPageStore = PageStore.create(PageStoreOptions.create(mConf), true);
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, mPageStore, mEvictor);
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
    mPageStore = PageStore.create(PageStoreOptions.create(mConf), true);
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, mPageStore, mEvictor);
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
    mPageStore = PageStore.create(PageStoreOptions.create(mConf), true);
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, mPageStore, mEvictor);
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
  public void evictSmallPagesByPutPigPage() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
    mPageStore = PageStore.create(PageStoreOptions.create(mConf), true);
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, mPageStore, mEvictor);
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
  public void evictBigPagesByPutSmallPage() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
    mPageStore = PageStore.create(PageStoreOptions.create(mConf), true);
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, mPageStore, mEvictor);
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
  public void putGetPartialPage() throws Exception {
    int[] sizeArray = {1, PAGE_SIZE_BYTES / 2, PAGE_SIZE_BYTES - 1};
    for (int size: sizeArray) {
      PageId pageId = new PageId("3", size);
      byte[] pageData = page(0, size);
      assertTrue(mCacheManager.put(pageId, pageData));
      byte[] buf = new byte[size];
      assertEquals(size, mCacheManager.get(pageId, size, buf, 0));
      assertArrayEquals(pageData, buf);
    }
  }

  @Test
  public void putMoreThanCacheCapacity() throws Exception {
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
    for (int offset: offsetArray) {
      assertEquals(PAGE1.length - offset,
          mCacheManager.get(PAGE_ID1, offset, PAGE1.length - offset, mBuf, 0));
      assertEquals(ByteBuffer.wrap(PAGE1, offset, PAGE1.length - offset),
          ByteBuffer.wrap(mBuf, 0, PAGE1.length - offset));
    }
  }

  @Test
  public void getNotEnoughSpaceException() throws Exception {
    byte[] buf = new byte[PAGE1.length - 1];
    mThrown.expect(IllegalArgumentException.class);
    mCacheManager.get(PAGE_ID1, PAGE1.length, buf, 0);
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
  public void restore() throws Exception {
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    mCacheManager.put(PAGE_ID1, PAGE1);
    mCacheManager.put(pageUuid, PAGE2);
    mCacheManager.close();
    mCacheManager = LocalCacheManager.create(mConf);
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    assertEquals(PAGE2.length, mCacheManager.get(pageUuid, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
  }

  @Test
  public void restoreFailed() throws Exception {
    PageId pageUuid = new PageId(UUID.randomUUID().toString(), 0);
    mCacheManager.put(PAGE_ID1, PAGE1);
    mCacheManager.put(pageUuid, PAGE2);
    mCacheManager.close();
    String rootDir = PageStore.getStorePath(PageStoreType.LOCAL,
        mConf.get(PropertyKey.USER_CLIENT_CACHE_DIR)).toString();
    FileUtils.createFile(Paths.get(rootDir, "invalidPageFile").toString());
    mCacheManager = LocalCacheManager.create(mConf);
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertEquals(0, mCacheManager.get(pageUuid, PAGE2.length, mBuf, 0));
  }

  @Test
  public void asyncCache() throws Exception {
    final int threads = 16;
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, true);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_THREADS, threads);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL");
    PutDelayedPageStore pageStore = new PutDelayedPageStore();
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, pageStore, mEvictor);
    for (int i = 0; i < threads; i++) {
      PageId pageId = new PageId("5", i);
      assertTrue(mCacheManager.put(pageId, page(i, PAGE_SIZE_BYTES)));
    }
    // fail due to full queue
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
    pageStore.setHanging(false);
    while (pageStore.getPuts() < threads) {
      Thread.sleep(1000);
    }
    pageStore.setHanging(true);
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
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, "LOCAL");
    PutDelayedPageStore pageStore = new PutDelayedPageStore();
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, pageStore, mEvictor);
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
    pageStore.setHanging(false);
    while (pageStore.getPuts() < 1) {
      Thread.sleep(1000);
    }
    pageStore.setHanging(true);
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
  }

  @Test
  public void recoverCacheFromFailedPut() throws Exception {
    FaultyPageStore pageStore = new FaultyPageStore();
    mCacheManager = new LocalCacheManager(mConf, mMetaStore, pageStore, mEvictor);
    pageStore.setFaulty(true);
    // a failed put
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
    // no state left after previous failed put
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    // can ask to put same page again without exception
    assertFalse(mCacheManager.put(PAGE_ID1, PAGE1));
    // still no state left
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    pageStore.setFaulty(false);
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertEquals(PAGE_SIZE_BYTES, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
  }

  /**
   * A PageStore where put can throw IOException on put.
   */
  private class FaultyPageStore extends LocalPageStore {
    public FaultyPageStore() {
      super(PageStoreOptions.create(mConf).toOptions());
    }

    private AtomicBoolean mFaulty = new AtomicBoolean(false);

    @Override
    public void put(PageId pageId, byte[] page) throws IOException {
      if (mFaulty.get()) {
        throw new IOException("Not found");
      }
      super.put(pageId, page);
    }

    void setFaulty(boolean faulty) {
      mFaulty.set(faulty);
    }
  }

  /**
   * A PageStore where put will always hang.
   */
  private class PutDelayedPageStore extends LocalPageStore {
    private AtomicBoolean mHanging = new AtomicBoolean(true);
    private AtomicInteger mPut = new AtomicInteger(0);

    public PutDelayedPageStore() {
      super(PageStoreOptions.create(mConf).toOptions());
    }

    @Override
    public void put(PageId pageId, byte[] page) throws IOException {
      // never quit
      while (mHanging.get()) {}
      super.put(pageId, page);
      mPut.getAndIncrement();
    }

    void setHanging(boolean value) {
      mHanging.set(value);
    }

    int getPuts() {
      return mPut.get();
    }
  }

  /**
   * Implementation of Evictor using FIFO eviction policy for the test.
   */
  class FIFOEvictor implements CacheEvictor {
    final Queue<PageId> mQueue = new LinkedList<>();
    final MetaStore  mMetaStore;

    public FIFOEvictor(MetaStore metaStore) {
      mMetaStore = metaStore;
    }

    @Override
    public void updateOnGet(PageId pageId) {
      // noop
    }

    @Override
    public void updateOnPut(PageId pageId) {
      mQueue.add(pageId);
    }

    @Override
    public void updateOnDelete(PageId pageId) {
      // noop
    }

    @Nullable
    @Override
    public PageId evict() {
      PageId pageId;
      while ((pageId = mQueue.peek()) != null) {
        if (mMetaStore.hasPage(pageId)) {
          return pageId;
        }
        // this page has been deleted
        mQueue.poll();
      }
      return null;
    }

    @Override
    public void reset() {
      mQueue.clear();
    }
  }
}
