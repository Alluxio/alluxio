/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file.meta;

import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link TtlBucketList}.
 */
public class TtlBucketListTest {
  private static final long BUCKET_INTERVAL = 10;
  private static final long BUCKET1_START = 0;
  private static final long BUCKET1_END = BUCKET1_START + BUCKET_INTERVAL;
  private static final long BUCKET2_START = BUCKET1_END;
  private static final long BUCKET2_END =  BUCKET2_START + BUCKET_INTERVAL;
  private static final InodeFile BUCKET1_FILE1 = new InodeFile.Builder().setCreationTimeMs(0)
      .setBlockContainerId(0).setTtl(BUCKET1_START).build();
  private static final InodeFile BUCKET1_FILE2 = new InodeFile.Builder().setCreationTimeMs(0)
      .setBlockContainerId(1).setTtl(BUCKET1_END - 1).build();
  private static final InodeFile BUCKET2_FILE = new InodeFile.Builder().setCreationTimeMs(0)
      .setBlockContainerId(2).setTtl(BUCKET2_START).build();
  private static long sOldTtlIntervalMs;

  private TtlBucketList mBucketList;

  /**
   * Sets up the TTL interval before a single test runs.
   */
  @BeforeClass
  public static void beforeClass() {
    sOldTtlIntervalMs = TtlBucket.getTtlIntervalMs();
    TtlBucketPrivateAccess.setTtlIntervalMs(BUCKET_INTERVAL);
  }

  /**
   * Resets the TTL interval after all test ran.
   */
  @AfterClass
  public static void afterClass() {
    TtlBucketPrivateAccess.setTtlIntervalMs(sOldTtlIntervalMs);
  }

  /**
   * Sets up a new {@link TtlBucketList} before a test runs.
   */
  @Before
  public void before() {
    mBucketList = new TtlBucketList();
  }

  private List<TtlBucket> getSortedExpiredBuckets(long expireTime) {
    List<TtlBucket> buckets = Lists.newArrayList(mBucketList.getExpiredBuckets(expireTime));
    Collections.sort(buckets);
    return buckets;
  }

  private void assertExpired(List<TtlBucket> expiredBuckets, int bucketIndex, InodeFile... files) {
    TtlBucket bucket = expiredBuckets.get(bucketIndex);
    Assert.assertEquals(files.length, bucket.getFiles().size());
    Assert.assertTrue(bucket.getFiles().containsAll(Lists.newArrayList(files)));
  }

  /**
   * Tests the {@link TtlBucketList#insert(InodeFile)} method.
   */
  @Test
  public void insertTest() {
    // No bucket should expire.
    List<TtlBucket> expired = getSortedExpiredBuckets(BUCKET1_START);
    Assert.assertTrue(expired.isEmpty());

    mBucketList.insert(BUCKET1_FILE1);
    // The first bucket should expire.
    expired = getSortedExpiredBuckets(BUCKET1_END);
    assertExpired(expired, 0, BUCKET1_FILE1);

    mBucketList.insert(BUCKET1_FILE2);
    // Only the first bucket should expire.
    for (long end = BUCKET2_START; end < BUCKET2_END; end ++) {
      expired = getSortedExpiredBuckets(end);
      assertExpired(expired, 0, BUCKET1_FILE1, BUCKET1_FILE2);
    }

    mBucketList.insert(BUCKET2_FILE);
    // All buckets should expire.
    expired = getSortedExpiredBuckets(BUCKET2_END);
    assertExpired(expired, 0, BUCKET1_FILE1, BUCKET1_FILE2);
    assertExpired(expired, 1, BUCKET2_FILE);
  }

  /**
   * Tests the {@link TtlBucketList#remove(InodeFile)} method.
   */
  @Test
  public void removeTest() {
    mBucketList.insert(BUCKET1_FILE1);
    mBucketList.insert(BUCKET1_FILE2);
    mBucketList.insert(BUCKET2_FILE);

    List<TtlBucket> expired = getSortedExpiredBuckets(BUCKET1_END);
    assertExpired(expired, 0, BUCKET1_FILE1, BUCKET1_FILE2);

    mBucketList.remove(BUCKET1_FILE1);
    expired = getSortedExpiredBuckets(BUCKET1_END);
    // Only the first bucket should expire, and there should be only one BUCKET1_FILE2 in it.
    assertExpired(expired, 0, BUCKET1_FILE2);

    mBucketList.remove(BUCKET1_FILE2);
    expired = getSortedExpiredBuckets(BUCKET1_END);
    // Only the first bucket should expire, and there should be no files in it.
    assertExpired(expired, 0); // nothing in bucket 0.

    expired = getSortedExpiredBuckets(BUCKET2_END);
    // All buckets should expire.
    assertExpired(expired, 0); // nothing in bucket 0.
    assertExpired(expired, 1, BUCKET2_FILE);

    // Remove bucket 0.
    expired = getSortedExpiredBuckets(BUCKET1_END);
    mBucketList.removeBuckets(Sets.newHashSet(expired));

    expired = getSortedExpiredBuckets(BUCKET2_END);
    // The only remaining bucket is bucket 1, it should expire.
    assertExpired(expired, 0, BUCKET2_FILE);

    mBucketList.remove(BUCKET2_FILE);
    expired = getSortedExpiredBuckets(BUCKET2_END);
    assertExpired(expired, 0); // nothing in bucket.

    mBucketList.removeBuckets(Sets.newHashSet(expired));
    // No bucket should exist now.
    expired = getSortedExpiredBuckets(BUCKET2_END);
    Assert.assertEquals(0, expired.size());
  }
}
