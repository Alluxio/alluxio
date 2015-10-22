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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TTLBucketTest {
  private TTLBucket mBucket;

  @Before
  public void before() {
    mBucket = new TTLBucket(0);
  }

  @Test
  public void intervalTest() {
    for (long i = 0; i < 10; i ++) {
      mBucket = new TTLBucket(i);
      Assert.assertEquals(mBucket.getTTLIntervalEndTimeMs(), mBucket.getTTLIntervalStartTimeMs()
          + TTLBucket.getTTLIntervalMs());
    }
  }

  @Test
  public void compareIntervalStartTimeTest() {
    for (long i = 0; i < 10; i ++) {
      for (long j = i + 1; j < 10; j ++) {
        TTLBucket bucket1 = new TTLBucket(i);
        Assert.assertEquals(i, bucket1.getTTLIntervalStartTimeMs());

        TTLBucket bucket2 = new TTLBucket(j);
        Assert.assertEquals(j, bucket2.getTTLIntervalStartTimeMs());

        Assert.assertEquals(-1, bucket1.compareTo(bucket2));
        Assert.assertEquals(1, bucket2.compareTo(bucket1));

        TTLBucket bucket3 = new TTLBucket(i);
        Assert.assertEquals(i, bucket3.getTTLIntervalStartTimeMs());

        Assert.assertEquals(0, bucket1.compareTo(bucket3));
      }
    }
  }

  @Test
  public void addAndRemoveFileTest() {
    InodeFile mFileTTL1 = new InodeFile.Builder().setCreationTimeMs(0).setBlockContainerId(0)
        .setTTL(1).build();
    InodeFile mFileTTL2 = new InodeFile.Builder().setCreationTimeMs(0).setBlockContainerId(1)
        .setTTL(2).build();
    Assert.assertTrue(mBucket.getFiles().isEmpty());

    mBucket.addFile(mFileTTL1);
    Assert.assertEquals(1, mBucket.getFiles().size());

    // The same file, won't be added.
    mBucket.addFile(mFileTTL1);
    Assert.assertEquals(1, mBucket.getFiles().size());

    // Different file, will be added.
    mBucket.addFile(mFileTTL2);
    Assert.assertEquals(2, mBucket.getFiles().size());

    // Remove files;
    mBucket.removeFile(mFileTTL1);
    Assert.assertEquals(1, mBucket.getFiles().size());
    Assert.assertTrue(mBucket.getFiles().contains(mFileTTL2));
    mBucket.removeFile(mFileTTL2);
    Assert.assertEquals(0, mBucket.getFiles().size());
  }
}
