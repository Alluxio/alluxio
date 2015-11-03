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
import org.junit.Test;

public class TTLBucketTest {

  @Test
  public void compareToTest() {
    TTLBucket firstBucket = new TTLBucket(0);
    TTLBucket secondBucket = new TTLBucket(0);
    TTLBucket thirdBucket = new TTLBucket(1);

    Assert.assertTrue(firstBucket.compareTo(firstBucket) == 0);
    Assert.assertTrue(firstBucket.compareTo(secondBucket) == 0
        && secondBucket.compareTo(firstBucket) == 0);
    Assert.assertFalse(firstBucket.compareTo(thirdBucket) == 0);
  }

  @Test
  public void equalsTest() {
    TTLBucket firstBucket = new TTLBucket(0);
    TTLBucket secondBucket = new TTLBucket(0);
    TTLBucket thirdBucket = new TTLBucket(1);

    Assert.assertFalse(firstBucket.equals(null));
    Assert.assertTrue(firstBucket.equals(firstBucket));
    Assert.assertTrue(firstBucket.equals(secondBucket) && secondBucket.equals(firstBucket));
    Assert.assertFalse(firstBucket.equals(thirdBucket));
  }

  @Test
  public void hashCodeTest() {
    TTLBucket firstBucket = new TTLBucket(0);
    TTLBucket secondBucket = new TTLBucket(0);
    TTLBucket thirdBucket = new TTLBucket(1);

    Assert.assertFalse(firstBucket.equals(null));
    Assert.assertTrue(firstBucket.hashCode() == secondBucket.hashCode());
    Assert.assertFalse(firstBucket.hashCode() == thirdBucket.hashCode());
  }

  @Test
  public void compareToWithFilesTest() {
    TTLBucket firstBucket = new TTLBucket(0);
    firstBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(1)
        .build());
    TTLBucket secondBucket = new TTLBucket(0);
    secondBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(1)
        .build());
    TTLBucket thirdBucket = new TTLBucket(0);
    TTLBucket fourthBucket = new TTLBucket(0);
    fourthBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(2)
        .build());

    Assert.assertTrue(firstBucket.compareTo(firstBucket) == 0);
    Assert.assertTrue(firstBucket.compareTo(secondBucket) == 0
        && secondBucket.compareTo(firstBucket) == 0);
    Assert.assertFalse(firstBucket.compareTo(thirdBucket) == 0);
    Assert.assertFalse(firstBucket.compareTo(fourthBucket) == 0);
  }

  @Test
  public void equalsWithFilesTest() {
    TTLBucket firstBucket = new TTLBucket(0);
    firstBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(1)
        .build());
    TTLBucket secondBucket = new TTLBucket(0);
    secondBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(1)
        .build());
    TTLBucket thirdBucket = new TTLBucket(0);
    TTLBucket fourthBucket = new TTLBucket(0);
    fourthBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(2)
        .build());

    Assert.assertTrue(firstBucket.equals(firstBucket));
    Assert.assertTrue(firstBucket.equals(secondBucket) && secondBucket.equals(firstBucket));
    Assert.assertFalse(firstBucket.equals(thirdBucket));
    Assert.assertFalse(firstBucket.equals(fourthBucket));
  }

  @Test
  public void hashCodeWithFilesTest() {
    TTLBucket firstBucket = new TTLBucket(0);
    firstBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(1)
        .build());
    TTLBucket secondBucket = new TTLBucket(0);
    secondBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(1)
        .build());
    TTLBucket thirdBucket = new TTLBucket(0);
    TTLBucket fourthBucket = new TTLBucket(0);
    fourthBucket.addFile(new InodeFile.Builder()
        .setBlockContainerId(2)
        .build());

    Assert.assertTrue(firstBucket.hashCode() == secondBucket.hashCode());
    Assert.assertFalse(firstBucket.hashCode() == thirdBucket.hashCode());
    Assert.assertFalse(firstBucket.hashCode() == fourthBucket.hashCode());
  }
}
