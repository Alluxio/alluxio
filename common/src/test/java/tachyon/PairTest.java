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

package tachyon;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.Pair
 */
public class PairTest {
  private String mStrType = "test";
  private Integer mIntegerType = 7883;
  private Long mLongType = 9887L;
  private Double mDoubleType = 3.14159;
  private Boolean mBoolType = false;
  private Character mCharType = 'a';
  private Object[] mObjs = new Object[6];

  @Test
  public void constructorTest() {
    mObjs[0] = mStrType;
    mObjs[1] = mIntegerType;
    mObjs[2] = mLongType;
    mObjs[3] = mDoubleType;
    mObjs[4] = mBoolType;
    mObjs[5] = mCharType;

    for (int j = 0; j < mObjs.length - 1; j ++) {
      for (int k = j + 1; k < mObjs.length; k ++) {
        Pair<Object, Object> tPair = new Pair<Object, Object>(mObjs[j], mObjs[k]);
        Assert.assertEquals(mObjs[j], tPair.getFirst());
        Assert.assertEquals(mObjs[k], tPair.getSecond());
        Assert.assertNotSame(mObjs[k], tPair.getFirst());
        Assert.assertNotSame(mObjs[j], tPair.getSecond());
      }
    }
  }
}
