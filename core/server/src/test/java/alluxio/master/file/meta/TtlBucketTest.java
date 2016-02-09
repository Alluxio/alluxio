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
  public void intervalTest() {
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
  public void compareIntervalStartTimeTest() {
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
   * Tests the {@link TtlBucket#addFile(InodeFile)} and {@link TtlBucket#removeFile(InodeFile)}
   * methods.
   */
  @Test
  public void addAndRemoveFileTest() {
    InodeFile mFileTtl1 = new InodeFile.Builder().setCreationTimeMs(0).setBlockContainerId(0)
        .setTtl(1).build();
    InodeFile mFileTtl2 = new InodeFile.Builder().setCreationTimeMs(0).setBlockContainerId(1)
        .setTtl(2).build();
    Assert.assertTrue(mBucket.getFiles().isEmpty());

    mBucket.addFile(mFileTtl1);
    Assert.assertEquals(1, mBucket.getFiles().size());

    // The same file, won't be added.
    mBucket.addFile(mFileTtl1);
    Assert.assertEquals(1, mBucket.getFiles().size());

    // Different file, will be added.
    mBucket.addFile(mFileTtl2);
    Assert.assertEquals(2, mBucket.getFiles().size());

    // Remove files;
    mBucket.removeFile(mFileTtl1);
    Assert.assertEquals(1, mBucket.getFiles().size());
    Assert.assertTrue(mBucket.getFiles().contains(mFileTtl2));
    mBucket.removeFile(mFileTtl2);
    Assert.assertEquals(0, mBucket.getFiles().size());
  }

  /**
   * Tests the {@link TtlBucket#compareTo(TtlBucket)} method.
   */
  @Test
  public void compareToTest() {
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
  public void equalsTest() {
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
