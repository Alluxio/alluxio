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

package alluxio.master.file.meta;

import static org.mockito.Mockito.mock;

import alluxio.master.metastore.InodeStore;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link TtlBucketList}.
 */
public final class TtlBucketListTest {
  private static final long BUCKET_INTERVAL = 10;
  private static final long BUCKET1_START = 0;
  private static final long BUCKET1_END = BUCKET1_START + BUCKET_INTERVAL;
  private static final long BUCKET2_START = BUCKET1_END;
  private static final long BUCKET2_END = BUCKET2_START + BUCKET_INTERVAL;
  private static final Inode BUCKET1_FILE1 =
      TtlTestUtils.createFileWithIdAndTtl(0, BUCKET1_START);
  private static final Inode BUCKET1_FILE2 =
      TtlTestUtils.createFileWithIdAndTtl(1, BUCKET1_END - 1);
  private static final Inode BUCKET2_FILE =
      TtlTestUtils.createFileWithIdAndTtl(2, BUCKET2_START);

  private TtlBucketList mBucketList;

  @ClassRule
  public static TtlIntervalRule sTtlIntervalRule = new TtlIntervalRule(BUCKET_INTERVAL);

  /**
   * Sets up a new {@link TtlBucketList} before a test runs.
   */
  @Before
  public void before() {
    mBucketList = new TtlBucketList(mock(InodeStore.class));
  }

  private List<TtlBucket> pollSortedExpiredBuckets(long expireTime) {
    List<TtlBucket> buckets = Lists.newArrayList(mBucketList.pollExpiredBuckets(expireTime));
    Collections.sort(buckets);
    return buckets;
  }

  private void assertExpired(List<TtlBucket> expiredBuckets, int bucketIndex,
      Inode... inodes) {
    TtlBucket bucket = expiredBuckets.get(bucketIndex);
    Assert.assertEquals(inodes.length, bucket.size());
    List<Long> inodeIds = Lists.newArrayList(inodes).stream().map(Inode::getId)
        .collect(Collectors.toList());
    Assert.assertTrue(bucket.getInodeIds().containsAll(inodeIds));
  }

  /**
   * Tests the {@link TtlBucketList#insert(Inode)} method.
   */
  @Test
  public void insert() {
    // No bucket should expire.
    List<TtlBucket> expired = pollSortedExpiredBuckets(BUCKET1_START);
    Assert.assertTrue(expired.isEmpty());

    mBucketList.insert(BUCKET1_FILE1);
    // The first bucket should expire.
    expired = pollSortedExpiredBuckets(BUCKET1_END);
    assertExpired(expired, 0, BUCKET1_FILE1);
    mBucketList.insert(BUCKET1_FILE1);

    mBucketList.insert(BUCKET1_FILE2);
    // Only the first bucket should expire.
    for (long end = BUCKET2_START; end < BUCKET2_END; end++) {
      expired = pollSortedExpiredBuckets(end);
      assertExpired(expired, 0, BUCKET1_FILE1, BUCKET1_FILE2);
      mBucketList.insert(BUCKET1_FILE1);
      mBucketList.insert(BUCKET1_FILE2);
    }

    mBucketList.insert(BUCKET2_FILE);
    // All buckets should expire.
    expired = pollSortedExpiredBuckets(BUCKET2_END);
    assertExpired(expired, 0, BUCKET1_FILE1, BUCKET1_FILE2);
    assertExpired(expired, 1, BUCKET2_FILE);
  }

  /**
   * Tests the {@link TtlBucketList#remove(InodeView)} method.
   */
  @Test
  public void remove() {
    mBucketList.insert(BUCKET1_FILE1);
    mBucketList.insert(BUCKET1_FILE2);
    mBucketList.insert(BUCKET2_FILE);

    List<TtlBucket> expired = pollSortedExpiredBuckets(BUCKET1_END);
    assertExpired(expired, 0, BUCKET1_FILE1, BUCKET1_FILE2);
    mBucketList.insert(BUCKET1_FILE1);
    mBucketList.insert(BUCKET1_FILE2);

    mBucketList.remove(BUCKET1_FILE1);
    expired = pollSortedExpiredBuckets(BUCKET1_END);
    // Only the first bucket should expire, and there should be only one BUCKET1_FILE2 in it.
    assertExpired(expired, 0, BUCKET1_FILE2);
    mBucketList.insert(BUCKET1_FILE2);

    mBucketList.remove(BUCKET1_FILE2);
    expired = pollSortedExpiredBuckets(BUCKET1_END);
    // Only the first bucket should expire, and there should be no files in it.
    assertExpired(expired, 0); // nothing in bucket 0.

    expired = pollSortedExpiredBuckets(BUCKET2_END);
    // Current bucket should expire.
    assertExpired(expired, 0, BUCKET2_FILE);

    // No bucket should exist now.
    expired = pollSortedExpiredBuckets(BUCKET2_END);
    Assert.assertEquals(0, expired.size());
  }
}
