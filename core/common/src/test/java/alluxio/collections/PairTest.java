/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.collections;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Pair}.
 */
public class PairTest {
  private static final String STR_TYPE = "test";
  private static final Integer INTEGER_TYPE = 7883;
  private static final Long LONG_TYPE = 9887L;
  private static final Double DOUBLE_TYPE = 3.14159;
  private static final Boolean BOOL_TYPE = false;
  private static final Character CHAR_TYPE = 'a';
  private Object[] mObjs = new Object[6];

  /**
   * Tests the {@link Pair} constructor.
   */
  @Test
  public void constructorTest() {
    mObjs[0] = STR_TYPE;
    mObjs[1] = INTEGER_TYPE;
    mObjs[2] = LONG_TYPE;
    mObjs[3] = DOUBLE_TYPE;
    mObjs[4] = BOOL_TYPE;
    mObjs[5] = CHAR_TYPE;

    for (int j = 0; j < mObjs.length - 1; j++) {
      for (int k = j + 1; k < mObjs.length; k++) {
        Pair<Object, Object> tPair = new Pair<Object, Object>(mObjs[j], mObjs[k]);
        Assert.assertEquals(mObjs[j], tPair.getFirst());
        Assert.assertEquals(mObjs[k], tPair.getSecond());
        Assert.assertNotSame(mObjs[k], tPair.getFirst());
        Assert.assertNotSame(mObjs[j], tPair.getSecond());
      }
    }
  }
}
