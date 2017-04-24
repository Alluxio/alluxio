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

import alluxio.util.CommonUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class JobConfInfoTest {

  /**
   * Test to convert between a JobConfInfo type and a json type.
   */
  @Test
  public void json() throws Exception {
    JobConfInfo jobConfInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    JobConfInfo other =
        mapper.readValue(mapper.writeValueAsBytes(jobConfInfo), JobConfInfo.class);
    checkEquality(jobConfInfo, other);
  }

  /**
   * Test to convert between a thrift type and a wire type.
   */
  @Test
  public void thrift() {
    JobConfInfo jobConfInfo = createRandom();
    JobConfInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(jobConfInfo));
    checkEquality(jobConfInfo, other);
  }

  /**
   * Check if the two JobConfInfo object are equal.
   *
   * @param a the first JobConfInfo object to be checked
   * @param b the second JobConfInfo object to be checked
   */
  public void checkEquality(JobConfInfo a, JobConfInfo b) {
    Assert.assertEquals(a.getOutputFile(), b.getOutputFile());
    Assert.assertEquals(a, b);
  }

  /**
   * Randomly create a JobConfInfo object.
   *
   * @return the created JobConfInfo object
   */
  public static JobConfInfo createRandom() {
    JobConfInfo result = new JobConfInfo();
    Random random = new Random();

    String outputFile = CommonUtils.randomAlphaNumString(random.nextInt(10));

    result.setOutputFile(outputFile);

    return result;
  }
}
