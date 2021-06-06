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
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.BufferUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

/**
 * Tests for the {@link LocalCacheManager} class.
 */
public final class CacheManagerWithShadowCacheTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final PageId PAGE_ID1 = new PageId("0L", 0L);
  private static final PageId PAGE_ID2 = new PageId("1L", 1L);
  private static final byte[] PAGE1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);
  private static final byte[] PAGE2 = BufferUtils.getIncreasingByteArray(255, PAGE_SIZE_BYTES);
  private final byte[] mBuf = new byte[PAGE_SIZE_BYTES];
  private CacheManagerWithShadowCache mCacheManager;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Before
  public void before() throws Exception {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SHADOW_WINDOW, "20s");
    mCacheManager = new CacheManagerWithShadowCache(new KVCacheManager(), mConf);
    mCacheManager.stopUpdate();
  }

  @Test
  public void putOne() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 1);
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1.length);
  }

  @Test
  public void putTwo() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 2);
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1.length + PAGE2.length);
  }

  @Test
  public void putExist() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE2));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 1);
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1.length);
  }

  @Test
  public void bloomFilterExpire() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    mCacheManager.switchBloomFilter();
    mCacheManager.switchBloomFilter();
    mCacheManager.switchBloomFilter();
    mCacheManager.switchBloomFilter();
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 0);
    assertEquals(mCacheManager.getShadowCacheBytes(), 0);
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
  }

  @Test
  public void BloomFilterExpireHalf() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    mCacheManager.switchBloomFilter();
    mCacheManager.switchBloomFilter();
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2));
    assertEquals(PAGE2.length, mCacheManager.get(PAGE_ID2, PAGE2.length, mBuf, 0));
    assertArrayEquals(PAGE2, mBuf);
    mCacheManager.switchBloomFilter();
    mCacheManager.switchBloomFilter();
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 1);
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE2.length);
  }

  @Test
  public void delete() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1));
    assertEquals(PAGE1.length, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    assertArrayEquals(PAGE1, mBuf);
    assertTrue(mCacheManager.delete(PAGE_ID1));
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0));
    mCacheManager.updateWorkingSetSize();
    mCacheManager.updateWorkingSetSize();
    mCacheManager.updateWorkingSetSize();
    assertEquals(1, mCacheManager.getShadowCachePages());
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1.length);
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
  public void hit() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1);
    mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0);
    assertEquals(1, mCacheManager.getShadowCachePageRead());
    assertEquals(1, mCacheManager.getShadowCachePageHit());
    assertEquals(PAGE1.length, mCacheManager.getShadowCacheByteRead());
    assertEquals(PAGE1.length, mCacheManager.getShadowCacheByteHit());
  }

  @Test
  public void hitAfterMiss() throws Exception {
    mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0);
    mCacheManager.put(PAGE_ID1, PAGE1);
    mCacheManager.delete(PAGE_ID1);
    mCacheManager.get(PAGE_ID1, PAGE1.length, mBuf, 0);
    assertEquals(2, mCacheManager.getShadowCachePageRead());
    assertEquals(1, mCacheManager.getShadowCachePageHit());
    assertEquals(PAGE1.length * 2, mCacheManager.getShadowCacheByteRead());
    assertEquals(PAGE1.length, mCacheManager.getShadowCacheByteHit());
  }

  private class KVCacheManager implements CacheManager {
    private final HashMap<PageId, byte[]> mCache = new HashMap<>();

    @Override
    public boolean put(PageId pageId, byte[] page, CacheScope cacheScope, CacheQuota cacheQuota) {
      if (!mCache.containsKey(pageId)) {
        mCache.put(pageId, page);
      }
      return true;
    }

    @Override
    public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
        int offsetInBuffer) {
      if (!mCache.containsKey(pageId)) {
        return 0;
      }
      byte[] page = mCache.get(pageId);
      if (bytesToRead >= 0) {
        System.arraycopy(page, pageOffset + 0, buffer, offsetInBuffer + 0, bytesToRead);
      }
      return bytesToRead;
    }

    @Override
    public boolean delete(PageId pageId) {
      if (mCache.containsKey(pageId)) {
        mCache.remove(pageId);
        return true;
      }
      return false;
    }

    @Override
    public State state() {
      return null;
    }

    @Override
    public void close() throws Exception {}
  }
}
