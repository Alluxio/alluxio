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

package alluxio.wire;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class CapacityTest {

  @Test
  public void json() throws Exception {
    Capacity capacity = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    Capacity other = mapper.readValue(mapper.writeValueAsBytes(capacity), Capacity.class);
    checkEquality(capacity, other);
  }

  public void checkEquality(Capacity a, Capacity b) {
    Assert.assertEquals(a.getTotal(), b.getTotal());
    Assert.assertEquals(a.getUsed(), b.getUsed());
    Assert.assertEquals(a, b);
  }

  public static Capacity createRandom() {
    Capacity result = new Capacity();
    Random random = new Random();

    long total = random.nextLong();
    long used = random.nextLong();

    result.setTotal(total);
    result.setUsed(used);

    return result;
  }
}
