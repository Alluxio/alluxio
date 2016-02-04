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

package alluxio.wire;

import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

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
