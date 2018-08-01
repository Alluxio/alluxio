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

import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.Random;

public final class CommandLineJobInfoTest {

  @Test
  public void json() throws Exception {
    CommandLineJobInfo jobInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    CommandLineJobInfo other =
        mapper.readValue(mapper.writeValueAsBytes(jobInfo), CommandLineJobInfo.class);
    checkEquality(jobInfo, other);
  }

  @Test
  public void thrift() {
    CommandLineJobInfo jobInfo = createRandom();
    CommandLineJobInfo other = CommandLineJobInfo.fromThrift(jobInfo.toThrift());
    checkEquality(jobInfo, other);
  }

  public void checkEquality(CommandLineJobInfo a, CommandLineJobInfo b) {
    assertEquals(a.getCommand(), b.getCommand());
    assertEquals(a.getConf(), b.getConf());
    assertEquals(a, b);
  }

  public static CommandLineJobInfo createRandom() {
    CommandLineJobInfo result = new CommandLineJobInfo();
    Random random = new Random();

    String command = CommonUtils.randomAlphaNumString(random.nextInt(10));
    JobConfInfo jobConfInfo = JobConfInfoTest.createRandom();

    result.setCommand(command);
    result.setConf(jobConfInfo);

    return result;
  }
}
