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

package alluxio.wire;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class JobConfInfoTest {

  @Test
  public void jsonTest() throws Exception {
    JobConfInfo jobConfInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    JobConfInfo other =
        mapper.readValue(mapper.writeValueAsBytes(jobConfInfo), JobConfInfo.class);
    checkEquality(jobConfInfo, other);
  }

  @Test
  public void thriftTest() {
    JobConfInfo jobConfInfo = createRandom();
    JobConfInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(jobConfInfo));
    checkEquality(jobConfInfo, other);
  }

  public void checkEquality(JobConfInfo a, JobConfInfo b) {
    Assert.assertEquals(a.getOutputFile(), b.getOutputFile());
    Assert.assertEquals(a, b);
  }

  public static JobConfInfo createRandom() {
    JobConfInfo result = new JobConfInfo();
    Random random = new Random();

    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    String outputFile = new String(bytes);

    result.setOutputFile(outputFile);

    return result;
  }
}
