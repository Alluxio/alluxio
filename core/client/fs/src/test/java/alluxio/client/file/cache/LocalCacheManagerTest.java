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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PageNotFoundException;
import alluxio.util.io.BufferUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import javax.annotation.Nullable;

/**
 * Tests for the {@link LocalCacheManager} class.
 */
public final class LocalCacheManagerTest {
  private static int PAGE_SIZE_BYTES = Constants.KB;
  private static int CACHE_SIZE_BYTES = 512 * Constants.KB;

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private FileSystemContext mFileContext;
  private HashSetMetaStore mMetaStore;
  private HashMapPageStore mPageStore;
  private CacheEvictor mEvictor;
  private final PageId mPageId1 = new PageId(0L, 0L);
  private final PageId mPageId2 = new PageId(1L, 1L);
  private final byte[] mPage1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);
  private final byte[] mPage2 = BufferUtils.getIncreasingByteArray(255, PAGE_SIZE_BYTES);

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES);
    mFileContext = Mockito.mock(FileSystemContext.class);
    when(mFileContext.getClusterConf()).thenReturn(mConf);
    when(mFileContext.getPathConf(any())).thenReturn(mConf);
    when(mFileContext.getUriValidationEnabled()).thenReturn(true);
    mMetaStore = new HashSetMetaStore();
    mPageStore = new HashMapPageStore();
    mEvictor = new FIFOEvictor(mMetaStore);
    mCacheManager = new LocalCacheManager(mFileContext, mMetaStore, mPageStore, mEvictor);
  }

  @Test
  public void putNew() throws Exception {
    mCacheManager.put(mPageId1, mPage1);
    assertEquals(1, mPageStore.size());
    assertTrue(mMetaStore.hasPage(mPageId1));
    assertArrayEquals(mPage1, (byte[]) mPageStore.mStore.get(mPageId1));
  }

  @Test
  public void putExist() throws Exception {
    mCacheManager.put(mPageId1, mPage1);
    mCacheManager.put(mPageId1, mPage2);
    assertEquals(1, mPageStore.size());
    assertTrue(mMetaStore.hasPage(mPageId1));
    assertArrayEquals(mPage2, (byte[]) mPageStore.mStore.get(mPageId1));
  }

  @Test
  public void putEvict() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
    mCacheManager = new LocalCacheManager(mFileContext, mMetaStore, mPageStore, mEvictor);
    mCacheManager.put(mPageId1, mPage1);
    assertEquals(1, mPageStore.size());
    mCacheManager.put(mPageId2, mPage2);
    assertEquals(1, mPageStore.size());
    assertFalse(mMetaStore.hasPage(mPageId1));
    assertTrue(mMetaStore.hasPage(mPageId2));
    assertArrayEquals(mPage2, (byte[]) mPageStore.mStore.get(mPageId2));
  }

  @Test
  public void putGetPartialPage() throws Exception {
    int[] sizeArray = {1, PAGE_SIZE_BYTES / 2, PAGE_SIZE_BYTES - 1};
    for (int size: sizeArray) {
      PageId pageId = new PageId(3, size);
      byte[] page = BufferUtils.getIncreasingByteArray(size);
      mCacheManager.put(pageId, page);
      try (ReadableByteChannel ret = mCacheManager.get(pageId)) {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE_BYTES);
        assertEquals(size, ret.read(buf));
        buf.flip();
        assertEquals(ByteBuffer.wrap(page), buf);
      }
    }
  }

  @Test
  public void putMoreThanCacheCapacity() throws Exception {
    int cacheSize = CACHE_SIZE_BYTES / PAGE_SIZE_BYTES;
    for (int i = 0; i < 2 * cacheSize; i++) {
      PageId pageId = new PageId(3, i);
      mCacheManager.put(pageId, BufferUtils.getIncreasingByteArray(i, PAGE_SIZE_BYTES));
      if (i >= cacheSize) {
        assertNull(mCacheManager.get(new PageId(3, i - cacheSize)));
        try (ReadableByteChannel ret = mCacheManager.get(new PageId(3, i - cacheSize + 1))) {
          ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE_BYTES);
          ret.read(buf);
          buf.flip();
          assertArrayEquals(
              BufferUtils.getIncreasingByteArray(i - cacheSize + 1, PAGE_SIZE_BYTES), buf.array());
        }
      }
    }
  }

  @Test
  public void getExist() throws Exception {
    mCacheManager.put(mPageId1, mPage1);
    try (ReadableByteChannel ret = mCacheManager.get(mPageId1)) {
      ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE_BYTES);
      assertEquals(mPage1.length, ret.read(buf));
      assertArrayEquals(mPage1, buf.array());
    }
  }

  @Test
  public void getNotExist() throws Exception {
    assertNull(mCacheManager.get(mPageId1));
  }

  @Test
  public void getOffset() throws Exception {
    mCacheManager.put(mPageId1, mPage1);
    int[] offsetArray = {0, 1, PAGE_SIZE_BYTES / 2, PAGE_SIZE_BYTES - 1};
    for (int offset: offsetArray) {
      try (ReadableByteChannel ret = mCacheManager.get(mPageId1, offset)) {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE_BYTES);
        assertEquals(mPage1.length - offset, ret.read(buf));
        buf.flip();
        assertEquals(ByteBuffer.wrap(mPage1, offset, mPage1.length - offset),
            buf);
      }
    }
  }

  @Test
  public void deleteExist() throws Exception {
    mCacheManager.put(mPageId1, mPage1);
    mCacheManager.delete(mPageId1);
    assertNull(mCacheManager.get(mPageId1));
  }

  @Test
  public void deleteNotExist() throws Exception {
    mThrown.expect(PageNotFoundException.class);
    mCacheManager.delete(mPageId1);
  }

  /**
   * Implementation of page store that stores cached data in memory.
   */
  class HashMapPageStore implements PageStore {

    HashMap mStore = new HashMap<PageId, byte[]>();

    public HashMapPageStore() {
    }

    @Override
    public void put(PageId pageId, byte[] page) throws IOException {
      mStore.put(pageId, page);
    }

    @Override
    public ReadableByteChannel get(PageId pageId) throws IOException, PageNotFoundException {
      byte[] buf = (byte[]) mStore.get(pageId);
      return Channels.newChannel(new ByteArrayInputStream(buf));
    }

    @Override
    public void delete(PageId pageId) throws IOException, PageNotFoundException {
      if (null == mStore.remove(pageId)) {
        throw new PageNotFoundException("not found key " + pageId);
      }
    }

    @Override
    public void close() {
      // noop
    }

    @Override
    public int size() {
      return mStore.size();
    }
  }

  /**
   * Implementation of meta store that stores cached data in memory.
   */
  class HashSetMetaStore implements MetaStore {
    HashSet mPages = new HashSet<PageId>();

    @Override
    public boolean hasPage(PageId pageId) {
      return mPages.contains(pageId);
    }

    @Override
    public boolean addPage(PageId pageId) {
      return mPages.add(pageId);
    }

    @Override
    public void removePage(PageId pageId) throws PageNotFoundException {
      if (!mPages.contains(pageId)) {
        throw new PageNotFoundException("page not found " + pageId);
      }
      mPages.remove(pageId);
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
  }
}
