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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link TtlBucket}.
 */
public class TtlBucketTest {
  private TtlBucket mBucket;

  /**
   * Sets up a new {@link TtlBucket} before a test runs.
   */
  @Before
  public void before() {
    mBucket = new TtlBucket(0);
  }

  /**
   * Tests the different interval methods.
   */
  @Test
  public void interval() {
    for (long i = 0; i < 10; i++) {
      mBucket = new TtlBucket(i);
      Assert.assertEquals(mBucket.getTtlIntervalEndTimeMs(), mBucket.getTtlIntervalStartTimeMs()
          + TtlBucket.getTtlIntervalMs());
    }
  }

  /**
   * Tests the {@link TtlBucket#compareTo(TtlBucket)} method with the
   * {@link TtlBucket#getTtlIntervalStartTimeMs()} method.
   */
  @Test
  public void compareIntervalStartTime() {
    for (long i = 0; i < 10; i++) {
      for (long j = i + 1; j < 10; j++) {
        TtlBucket bucket1 = new TtlBucket(i);
        Assert.assertEquals(i, bucket1.getTtlIntervalStartTimeMs());

        TtlBucket bucket2 = new TtlBucket(j);
        Assert.assertEquals(j, bucket2.getTtlIntervalStartTimeMs());

        Assert.assertEquals(-1, bucket1.compareTo(bucket2));
        Assert.assertEquals(1, bucket2.compareTo(bucket1));

        TtlBucket bucket3 = new TtlBucket(i);
        Assert.assertEquals(i, bucket3.getTtlIntervalStartTimeMs());

        Assert.assertEquals(0, bucket1.compareTo(bucket3));
      }
    }
  }

  /**
   * Tests the {@link TtlBucket#addInode(Inode)} and
   * {@link TtlBucket#removeInode(InodeView)} methods.
   */
  @Test
  public void addAndRemoveInodeFile() {
    Inode mFileTtl1 = TtlTestUtils.createFileWithIdAndTtl(0, 1);
    Inode mFileTtl2 = TtlTestUtils.createFileWithIdAndTtl(1, 2);
    Assert.assertTrue(mBucket.getInodes().isEmpty());

    mBucket.addInode(mFileTtl1);
    Assert.assertEquals(1, mBucket.getInodes().size());

    // The same file, won't be added.
    mBucket.addInode(mFileTtl1);
    Assert.assertEquals(1, mBucket.getInodes().size());

    // Different file, will be added.
    mBucket.addInode(mFileTtl2);
    Assert.assertEquals(2, mBucket.getInodes().size());

    // Remove files;
    mBucket.removeInode(mFileTtl1);
    Assert.assertEquals(1, mBucket.getInodes().size());
    Assert.assertTrue(mBucket.getInodes().contains(mFileTtl2));
    mBucket.removeInode(mFileTtl2);
    Assert.assertEquals(0, mBucket.getInodes().size());
  }

  /**
   * Tests the {@link TtlBucket#addInode(Inode)} and
   * {@link TtlBucket#removeInode(InodeView)} methods.
   */
  @Test
  public void addAndRemoveInodeDirectory() {
    Inode mDirectoryTtl1 = TtlTestUtils.createDirectoryWithIdAndTtl(0, 1);
    Inode mDirectoryTtl2 = TtlTestUtils.createDirectoryWithIdAndTtl(1, 2);
    Assert.assertTrue(mBucket.getInodes().isEmpty());

    mBucket.addInode(mDirectoryTtl1);
    Assert.assertEquals(1, mBucket.getInodes().size());

    // The same directory, won't be added.
    mBucket.addInode(mDirectoryTtl1);
    Assert.assertEquals(1, mBucket.getInodes().size());

    // Different directory, will be added.
    mBucket.addInode(mDirectoryTtl2);
    Assert.assertEquals(2, mBucket.getInodes().size());

    // Remove directorys;
    mBucket.removeInode(mDirectoryTtl1);
    Assert.assertEquals(1, mBucket.getInodes().size());
    Assert.assertTrue(mBucket.getInodes().contains(mDirectoryTtl2));
    mBucket.removeInode(mDirectoryTtl2);
    Assert.assertEquals(0, mBucket.getInodes().size());
  }

  /**
   * Tests the {@link TtlBucket#compareTo(TtlBucket)} method.
   */
  @Test
  public void compareTo() {
    TtlBucket firstBucket = new TtlBucket(0);
    TtlBucket secondBucket = new TtlBucket(0);
    TtlBucket thirdBucket = new TtlBucket(1);
    TtlBucket fourthBucket = new TtlBucket(2);

    Assert.assertEquals(0, firstBucket.compareTo(firstBucket));
    Assert.assertEquals(0, firstBucket.compareTo(secondBucket));
    Assert.assertEquals(0, secondBucket.compareTo(firstBucket));
    Assert.assertEquals(-1, firstBucket.compareTo(thirdBucket));
    Assert.assertEquals(1, fourthBucket.compareTo(firstBucket));
  }

  /**
   * Tests the {@link TtlBucket#equals(Object)} method.
   */
  @Test
  public void equals() {
    TtlBucket firstBucket = new TtlBucket(0);
    TtlBucket secondBucket = new TtlBucket(0);
    TtlBucket thirdBucket = new TtlBucket(1);

    Assert.assertNotEquals(firstBucket, null);
    Assert.assertEquals(firstBucket, firstBucket);
    Assert.assertEquals(firstBucket, secondBucket);
    Assert.assertEquals(secondBucket, firstBucket);
    Assert.assertNotEquals(firstBucket, thirdBucket);
  }

  /**
   * Tests the {@link TtlBucket#hashCode()} method.
   */
  @Test
  public void hashCodeTest() {
    TtlBucket firstBucket = new TtlBucket(0);
    TtlBucket secondBucket = new TtlBucket(0);
    TtlBucket thirdBucket = new TtlBucket(1);

    Assert.assertEquals(firstBucket.hashCode(), firstBucket.hashCode());
    Assert.assertEquals(firstBucket.hashCode(), secondBucket.hashCode());
    Assert.assertNotEquals(firstBucket.hashCode(), thirdBucket.hashCode());
  }
}
