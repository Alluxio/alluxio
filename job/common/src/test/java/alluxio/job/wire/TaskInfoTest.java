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

package alluxio.job.wire;

import static org.junit.Assert.assertEquals;

import alluxio.util.CommonUtils;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Tests the wire format {@link TaskInfo}.
 */
public final class TaskInfoTest {

  @Test
  public void testToProto() throws IOException {
    TaskInfo taskInfo = createRandom();

    assertEquals(taskInfo, new TaskInfo(taskInfo.toProto()));
  }

  public static TaskInfo createRandom() {
    TaskInfo result = new TaskInfo();
    Random random = new Random();

    result.setErrorMessage(CommonUtils.randomAlphaNumString(random.nextInt(10)));
    result.setStatus(Status.values()[random.nextInt(Status.values().length)]);
    result.setTaskId(random.nextInt());
    result.setDescription(CommonUtils.randomAlphaNumString(random.nextInt(10)));
    result.setWorkerHost("test");

    return result;
  }
}
