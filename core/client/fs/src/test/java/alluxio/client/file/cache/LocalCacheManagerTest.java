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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import javax.annotation.Nullable;

/**
 * Tests for the {@link LocalCacheManager} class.
 */
public final class LocalCacheManagerTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int CACHE_SIZE_BYTES = 512 * Constants.KB;
  private static final PageId PAGE_ID1 = new PageId(0L, 0L);
  private static final PageId PAGE_ID2 = new PageId(1L, 1L);
  private static final byte[] PAGE1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);
  private static final byte[] PAGE2 = BufferUtils.getIncreasingByteArray(255, PAGE_SIZE_BYTES);

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();
  private FileSystemContext mFileContext;
  private HashSetMetaStore mMetaStore;
  private HashMapPageStore mPageStore;
  private CacheEvictor mEvictor;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
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
    mCacheManager.put(PAGE_ID1, PAGE1);
    assertEquals(1, mPageStore.size());
    assertTrue(mMetaStore.hasPage(PAGE_ID1));
    assertArrayEquals(PAGE1, (byte[]) mPageStore.mStore.get(PAGE_ID1));
  }

  @Test
  public void putExist() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1);
    mCacheManager.put(PAGE_ID1, PAGE2);
    assertEquals(1, mPageStore.size());
    assertTrue(mMetaStore.hasPage(PAGE_ID1));
    assertArrayEquals(PAGE2, (byte[]) mPageStore.mStore.get(PAGE_ID1));
  }

  @Test
  public void putEvict() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, PAGE_SIZE_BYTES);
    mCacheManager = new LocalCacheManager(mFileContext, mMetaStore, mPageStore, mEvictor);
    mCacheManager.put(PAGE_ID1, PAGE1);
    assertEquals(1, mPageStore.size());
    mCacheManager.put(PAGE_ID2, PAGE2);
    assertEquals(1, mPageStore.size());
    assertFalse(mMetaStore.hasPage(PAGE_ID1));
    assertTrue(mMetaStore.hasPage(PAGE_ID2));
    assertArrayEquals(PAGE2, (byte[]) mPageStore.mStore.get(PAGE_ID2));
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
  public void putLargeId() throws Exception {
    long[] fileIdArray = {Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE + 1};
    long[] pageIndexArray = {Long.MAX_VALUE - 1, Long.MAX_VALUE};
    for (long fileId : fileIdArray) {
      for (long pageIndexId : pageIndexArray) {
        PageId largeId = new PageId(fileId, pageIndexId);
        mCacheManager.put(largeId, PAGE1);
        assertTrue(mMetaStore.hasPage(largeId));
        assertArrayEquals(PAGE1, (byte[]) mPageStore.mStore.get(largeId));
      }
    }
  }

  @Test
  public void getExist() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1);
    try (ReadableByteChannel ret = mCacheManager.get(PAGE_ID1)) {
      ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE_BYTES);
      assertEquals(PAGE1.length, ret.read(buf));
      assertArrayEquals(PAGE1, buf.array());
    }
  }

  @Test
  public void getNotExist() throws Exception {
    assertNull(mCacheManager.get(PAGE_ID1));
  }

  @Test
  public void getOffset() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1);
    int[] offsetArray = {0, 1, PAGE_SIZE_BYTES / 2, PAGE_SIZE_BYTES - 1};
    for (int offset: offsetArray) {
      try (ReadableByteChannel ret = mCacheManager.get(PAGE_ID1, offset)) {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE_BYTES);
        assertEquals(PAGE1.length - offset, ret.read(buf));
        buf.flip();
        assertEquals(ByteBuffer.wrap(PAGE1, offset, PAGE1.length - offset),
            buf);
      }
    }
  }

  @Test
  public void deleteExist() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1);
    mCacheManager.delete(PAGE_ID1);
    assertNull(mCacheManager.get(PAGE_ID1));
  }

  @Test
  public void deleteNotExist() throws Exception {
    mThrown.expect(PageNotFoundException.class);
    mCacheManager.delete(PAGE_ID1);
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
    public ReadableByteChannel get(PageId pageId, int pageOffset)
        throws IOException, PageNotFoundException {
      byte[] buf = (byte[]) mStore.get(pageId);
      ByteArrayInputStream is = new ByteArrayInputStream(buf);
      is.skip(pageOffset);
      return Channels.newChannel(is);
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

    @Override
    public Collection<PageId> getPages() {
      return mStore.keySet();
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
