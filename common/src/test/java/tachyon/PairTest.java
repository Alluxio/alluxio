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
  private static final String STRTYPE = "test";
  private static final Integer INTEGERTYPE = 7883;
  private static final Long LONGTYPE = 9887L;
  private static final Double DOUBLETYPE = 3.14159;
  private static final Boolean BOOLTYPE = false;
  private static final Character CHARTYPE = 'a';
  private Object[] mObjs = new Object[6];

  @Test
  public void constructorTest() {
    mObjs[0] = STRTYPE;
    mObjs[1] = INTEGERTYPE;
    mObjs[2] = LONGTYPE;
    mObjs[3] = DOUBLETYPE;
    mObjs[4] = BOOLTYPE;
    mObjs[5] = CHARTYPE;

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
