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

import static org.junit.Assert.assertEquals;

import alluxio.test.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Random;

public class CapacityTest {

  /**
   * Test to convert between a Capacity type and a json type.
   */
  @Test
  public void json() throws Exception {
    Capacity capacity = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    Capacity other = mapper.readValue(mapper.writeValueAsBytes(capacity), Capacity.class);
    checkEquality(capacity, other);
  }

  @Test
  public void equals() {
    CommonUtils.testEquals(AlluxioMasterInfo.class);
  }

  /**
   * Checks if the two Capacity objects are equal.
   *
   * @param a the first Capacity object to be checked
   * @param b the second Capacity object to be checked
   */
  private void checkEquality(Capacity a, Capacity b) {
    assertEquals(a.getTotal(), b.getTotal());
    assertEquals(a.getUsed(), b.getUsed());
    assertEquals(a, b);
  }

  /**
   * Randomly creates a Capacity object.
   *
   * @return the created Capacity object
   */
  protected static Capacity createRandom() {
    Capacity result = new Capacity();
    Random random = new Random();

    long total = random.nextLong();
    long used = random.nextLong();

    result.setTotal(total);
    result.setUsed(used);

    return result;
  }
}
