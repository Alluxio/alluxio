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
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link FIFOCacheEvictor} class.
 */
public final class FIFOCacheEvictorTest {
  private final FIFOCacheEvictor mEvictor = new FIFOCacheEvictor(ConfigurationTestUtils.defaults());
  private final PageId mFirst = new PageId("1L", 2L);
  private final PageId mSecond = new PageId("3L", 4L);
  private final PageId mThird = new PageId("5L", 6L);
  private final PageId mFourth = new PageId("7L", 8L);

  @Test
  public void evictPutOrder() {
    mEvictor.updateOnPut(mFirst);
    mEvictor.updateOnPut(mSecond);
    mEvictor.updateOnPut(mThird);
    mEvictor.updateOnPut(mFourth);
    Assert.assertEquals(mFirst, mEvictor.evict());
  }

  @Test
  public void evictPutAndGetOrder() {
    mEvictor.updateOnPut(mFirst);
    mEvictor.updateOnPut(mSecond);
    mEvictor.updateOnPut(mThird);
    mEvictor.updateOnPut(mFourth);
    mEvictor.updateOnGet(mFourth);
    mEvictor.updateOnGet(mFourth);
    mEvictor.updateOnGet(mFourth);
    Assert.assertEquals(mFirst, mEvictor.evict());
  }

  @Test
  public void evictAfterDelete() {
    mEvictor.updateOnPut(mFirst);
    mEvictor.updateOnPut(mSecond);
    mEvictor.updateOnPut(mThird);
    mEvictor.updateOnPut(mFourth);
    mEvictor.updateOnDelete(mSecond);
    Assert.assertEquals(mFirst, mEvictor.evict());
    mEvictor.updateOnDelete(mFirst);
    Assert.assertEquals(mThird, mEvictor.evict());
  }

  @Test
  public void evictEmpty() {
    Assert.assertNull(mEvictor.evict());
  }

  @Test
  public void evictAllGone() {
    mEvictor.updateOnPut(mFirst);
    mEvictor.updateOnPut(mSecond);
    mEvictor.updateOnPut(mThird);
    mEvictor.updateOnPut(mFourth);
    mEvictor.updateOnDelete(mFirst);
    mEvictor.updateOnDelete(mSecond);
    mEvictor.updateOnDelete(mThird);
    mEvictor.updateOnDelete(mFourth);
    Assert.assertNull(mEvictor.evict());
  }
}
