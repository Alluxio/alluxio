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

import alluxio.util.CommonUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class CommandLineJobInfoTest {

  @Test
  public void jsonTest() throws Exception {
    CommandLineJobInfo jobInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    CommandLineJobInfo other =
        mapper.readValue(mapper.writeValueAsBytes(jobInfo), CommandLineJobInfo.class);
    checkEquality(jobInfo, other);
  }

  @Test
  public void thriftTest() {
    CommandLineJobInfo jobInfo = createRandom();
    CommandLineJobInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(jobInfo));
    checkEquality(jobInfo, other);
  }

  public void checkEquality(CommandLineJobInfo a, CommandLineJobInfo b) {
    Assert.assertEquals(a.getCommand(), b.getCommand());
    Assert.assertEquals(a.getConf(), b.getConf());
    Assert.assertEquals(a, b);
  }

  public static CommandLineJobInfo createRandom() {
    CommandLineJobInfo result = new CommandLineJobInfo();
    Random random = new Random();

    String command = CommonUtils.randomString(random.nextInt(10));
    JobConfInfo jobConfInfo = JobConfInfoTest.createRandom();

    result.setCommand(command);
    result.setConf(jobConfInfo);

    return result;
  }
}
