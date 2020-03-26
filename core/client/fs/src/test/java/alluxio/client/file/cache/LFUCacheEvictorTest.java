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

import alluxio.ConfigurationTestUtils;
import alluxio.client.file.cache.evictor.LFUCacheEvictor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link LFUCacheEvictor} class.
 */
public final class LFUCacheEvictorTest {
  private LFUCacheEvictor mEvictor;
  private final PageId mOne = new PageId("1L", 2L);
  private final PageId mTwo = new PageId("3L", 4L);
  private final PageId mThree = new PageId("5L", 6L);
  private final PageId mFour = new PageId("7L", 8L);

  /**
   * Sets up the instances.
   */
  @Before
  public void before() {
    mEvictor = new LFUCacheEvictor(ConfigurationTestUtils.defaults());
  }

  @Test
  public void evictGetOrder() {
    mEvictor.updateOnGet(mOne);
    mEvictor.updateOnGet(mTwo);
    mEvictor.updateOnGet(mTwo);
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mFour);
    Assert.assertEquals(mOne, mEvictor.evict());
    mEvictor.updateOnDelete(mOne);
    Assert.assertEquals(mTwo, mEvictor.evict());
    mEvictor.updateOnDelete(mTwo);
    Assert.assertEquals(mFour, mEvictor.evict());
    mEvictor.updateOnDelete(mFour);
  }

  @Test
  public void evictGetMixedOrder() {
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mTwo);
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mTwo);
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mOne);
    mEvictor.updateOnGet(mFour);
    Assert.assertEquals(mOne, mEvictor.evict());
    mEvictor.updateOnDelete(mOne);
    Assert.assertEquals(mTwo, mEvictor.evict());
    mEvictor.updateOnDelete(mTwo);
    Assert.assertEquals(mFour, mEvictor.evict());
    mEvictor.updateOnDelete(mFour);
  }

  @Test
  public void evictGetRecentOrder() {
    mEvictor.updateOnGet(mOne);
    mEvictor.updateOnGet(mTwo);
    Assert.assertEquals(mOne, mEvictor.evict());
    mEvictor.updateOnDelete(mOne);
    Assert.assertEquals(mTwo, mEvictor.evict());
  }

  @Test
  public void evictUpdatedGetRecentOrder() {
    mEvictor.updateOnGet(mOne);
    mEvictor.updateOnGet(mOne);
    mEvictor.updateOnGet(mTwo);
    mEvictor.updateOnGet(mTwo);
    mEvictor.updateOnGet(mThree);
    mEvictor.updateOnGet(mThree);
    mEvictor.updateOnGet(mTwo);
    mEvictor.updateOnGet(mThree);
    mEvictor.updateOnGet(mOne);
    Assert.assertEquals(mTwo, mEvictor.evict());
    mEvictor.updateOnDelete(mTwo);
    Assert.assertEquals(mThree, mEvictor.evict());
    mEvictor.updateOnDelete(mThree);
    Assert.assertEquals(mOne, mEvictor.evict());
  }

  @Test
  public void evictDeleteSkippedBucket() {
    mEvictor.updateOnGet(mOne);
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mFour);
    mEvictor.updateOnGet(mFour);
    Assert.assertEquals(mOne, mEvictor.evict());
    mEvictor.updateOnDelete(mOne);
    Assert.assertEquals(mFour, mEvictor.evict());
    mEvictor.updateOnDelete(mFour);
  }

  @Test
  public void evictDeleteTwice() {
    mEvictor.updateOnGet(mOne);
    mEvictor.updateOnGet(mOne);
    Assert.assertEquals(mOne, mEvictor.evict());
    mEvictor.updateOnDelete(mOne);
    mEvictor.updateOnGet(mOne);
    Assert.assertEquals(mOne, mEvictor.evict());
  }

  @Test
  public void evictAfterDelete() {
    mEvictor.updateOnPut(mOne);
    mEvictor.updateOnPut(mTwo);
    mEvictor.updateOnPut(mTwo);
    mEvictor.updateOnPut(mThree);
    mEvictor.updateOnPut(mThree);
    mEvictor.updateOnPut(mThree);
    mEvictor.updateOnDelete(mTwo);
    Assert.assertEquals(mOne, mEvictor.evict());
    mEvictor.updateOnDelete(mOne);
    Assert.assertEquals(mThree, mEvictor.evict());
  }

  @Test
  public void evictEmpty() {
    Assert.assertNull(mEvictor.evict());
  }

  @Test
  public void evictAllGone() {
    mEvictor.updateOnPut(mOne);
    mEvictor.updateOnPut(mTwo);
    mEvictor.updateOnPut(mTwo);
    mEvictor.updateOnPut(mThree);
    mEvictor.updateOnPut(mThree);
    mEvictor.updateOnPut(mThree);
    mEvictor.updateOnDelete(mOne);
    mEvictor.updateOnDelete(mTwo);
    mEvictor.updateOnDelete(mThree);
    Assert.assertNull(mEvictor.evict());
  }
}
