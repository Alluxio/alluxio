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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.quota.CacheScope;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link ClockCuckooShadowCacheManager} class.
 */
public final class MultipleBloomShadowCacheManagerTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int BLOOMFILTER_NUM = 4;
  private static final int MAX_AGE = BLOOMFILTER_NUM;
  private static final PageId PAGE_ID1 = new PageId("0L", 0L);
  private static final PageId PAGE_ID2 = new PageId("1L", 1L);
  private static final int PAGE1_BYTES = PAGE_SIZE_BYTES;
  private static final int PAGE2_BYTES = PAGE_SIZE_BYTES + 1;
  private static final CacheScope SCOPE1 = CacheScope.create("schema1.table1");
  private static final CacheScope SCOPE2 = CacheScope.create("schema1.table2");
  private MultipleBloomShadowCacheManager mCacheManager;
  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Before
  public void before() {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SHADOW_BLOOMFILTER_NUM, BLOOMFILTER_NUM);
    mCacheManager = new MultipleBloomShadowCacheManager(mConf);
    mCacheManager.stopUpdate();
  }

  @Test
  public void putOne() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertEquals(PAGE1_BYTES, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 1);
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1_BYTES);
  }

  @Test
  public void putTwo() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2_BYTES, SCOPE1));
    assertEquals(PAGE1_BYTES, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertEquals(PAGE2_BYTES, mCacheManager.get(PAGE_ID2, PAGE2_BYTES, SCOPE1));
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 2);
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1_BYTES + PAGE2_BYTES);
  }

  @Test
  public void putExist() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE2_BYTES, SCOPE1));
    assertEquals(PAGE1_BYTES, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 1);
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1_BYTES);
  }

  @Test
  public void cuckooFilterExpire() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertEquals(PAGE1_BYTES, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    for (int i = 0; i < MAX_AGE; i++) {
      mCacheManager.aging();
    }
    mCacheManager.aging();
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 0);
    assertEquals(mCacheManager.getShadowCacheBytes(), 0);
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
  }

  @Test
  public void CuckooFilterExpireHalf() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertEquals(PAGE1_BYTES, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    for (int i = 0; i < MAX_AGE / 2; i++) {
      mCacheManager.aging();
    }
    assertTrue(mCacheManager.put(PAGE_ID2, PAGE2_BYTES, SCOPE1));
    assertEquals(PAGE2_BYTES, mCacheManager.get(PAGE_ID2, PAGE2_BYTES, SCOPE1));
    for (int i = 0; i < MAX_AGE / 2; i++) {
      mCacheManager.aging();
    }
    mCacheManager.updateWorkingSetSize();
    assertEquals(mCacheManager.getShadowCachePages(), 1);
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE2_BYTES);
  }

  @Test
  public void delete() throws Exception {
    assertTrue(mCacheManager.put(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertEquals(PAGE1_BYTES, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertFalse(mCacheManager.delete(PAGE_ID1));
    assertEquals(PAGE1_BYTES, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    mCacheManager.updateWorkingSetSize();
    // bloom filter does not support deleting, so PAGE_ID1 is not deleted actually
    assertEquals(1, mCacheManager.getShadowCachePages());
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1_BYTES);
  }

  @Test
  public void getExistInWindow() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1_BYTES, SCOPE1);
    assertEquals(PAGE1_BYTES, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE1_BYTES);
  }

  @Test
  public void getExistInRollingWindow() throws Exception {
    mCacheManager.put(PAGE_ID1, PAGE1_BYTES, SCOPE1);
    for (int i = 0; i < MAX_AGE; i++) {
      mCacheManager.aging();
    }
    mCacheManager.put(PAGE_ID2, PAGE2_BYTES, SCOPE1);
    // PAGE_ID1 is evicted, only PAGE_ID2 in the shadow cache
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE2_BYTES);
    // PAGE_ID1 is not in the shadow cache and `read` will not add it to shadow cache
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
    // PAGE_ID1 is not added to the shadow cache again by 'read'
    assertEquals(mCacheManager.getShadowCacheBytes(), PAGE2_BYTES);
  }

  @Test
  public void getNotExist() throws Exception {
    assertEquals(0, mCacheManager.get(PAGE_ID1, PAGE1_BYTES, SCOPE1));
  }
}
