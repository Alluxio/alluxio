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
    TTLBucket fourthBucket = new TTLBucket(2);

    Assert.assertEquals(0, firstBucket.compareTo(firstBucket));
    Assert.assertEquals(0, firstBucket.compareTo(secondBucket));
    Assert.assertEquals(0, secondBucket.compareTo(firstBucket));
    Assert.assertEquals(-1, firstBucket.compareTo(thirdBucket));
    Assert.assertEquals(1, fourthBucket.compareTo(firstBucket));
  }

  @Test
  public void equalsTest() {
    TTLBucket firstBucket = new TTLBucket(0);
    TTLBucket secondBucket = new TTLBucket(0);
    TTLBucket thirdBucket = new TTLBucket(1);

    Assert.assertNotEquals(firstBucket, null);
    Assert.assertEquals(firstBucket, firstBucket);
    Assert.assertEquals(firstBucket, secondBucket);
    Assert.assertEquals(secondBucket, firstBucket);
    Assert.assertNotEquals(firstBucket, thirdBucket);
  }

  @Test
  public void hashCodeTest() {
    TTLBucket firstBucket = new TTLBucket(0);
    TTLBucket secondBucket = new TTLBucket(0);
    TTLBucket thirdBucket = new TTLBucket(1);

    Assert.assertEquals(firstBucket.hashCode(), firstBucket.hashCode());
    Assert.assertEquals(firstBucket.hashCode(), secondBucket.hashCode());
    Assert.assertNotEquals(firstBucket.hashCode(), thirdBucket.hashCode());
  }
}
