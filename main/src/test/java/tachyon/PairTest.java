/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for tachyon.Pair
 */
public class PairTest {
  private String strType = "test";
  private Integer integerType = 7883;
  private Long longType = 9887L;
  private Double doubleType = 3.14159;
  private Boolean boolType = false;
  private Character charType = 'a';
  private Object[] obj = new Object[6];

  @Test
  public void constructorTest() {
    obj[0] = strType;
    obj[1] = integerType;
    obj[2] = longType;
    obj[3] = doubleType;
    obj[4] = boolType;
    obj[5] = charType;

    for (int j = 0; j < obj.length - 1; j ++) {
      for (int k = j + 1; k < obj.length; k ++) {
        Pair<Object, Object> tPair = new Pair<Object, Object>(obj[j], obj[k]);
        Assert.assertEquals(obj[j], tPair.getFirst());
        Assert.assertEquals(obj[k], tPair.getSecond());
        Assert.assertNotSame(obj[k], tPair.getFirst());
        Assert.assertNotSame(obj[j], tPair.getSecond());
      }
    }
  }
}