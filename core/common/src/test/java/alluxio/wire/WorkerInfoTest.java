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

public class WorkerInfoTest {

  @Test
  public void json() throws Exception {
    WorkerInfo workerInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    WorkerInfo other =
        mapper.readValue(mapper.writeValueAsBytes(workerInfo), WorkerInfo.class);
    checkEquality(workerInfo, other);
  }

  @Test
  public void thrift() {
    WorkerInfo workerInfo = createRandom();
    WorkerInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(workerInfo));
    checkEquality(workerInfo, other);
  }

  public void checkEquality(WorkerInfo a, WorkerInfo b) {
    Assert.assertEquals(a.getId(), b.getId());
    Assert.assertEquals(a.getAddress(), b.getAddress());
    Assert.assertEquals(a.getLastContactSec(), b.getLastContactSec());
    Assert.assertEquals(a.getState(), b.getState());
    Assert.assertEquals(a.getCapacityBytes(), b.getCapacityBytes());
    Assert.assertEquals(a.getUsedBytes(), b.getUsedBytes());
    Assert.assertEquals(a.getStartTimeMs(), b.getStartTimeMs());
    Assert.assertEquals(a, b);
  }

  public static WorkerInfo createRandom() {
    WorkerInfo result = new WorkerInfo();
    Random random = new Random();

    long id = random.nextLong();
    WorkerNetAddress address = WorkerNetAddressTest.createRandom();
    int lastContactSec = random.nextInt();
    String state = CommonUtils.randomAlphaNumString(random.nextInt(10));
    long capacityBytes = random.nextLong();
    long usedBytes = random.nextLong();
    long startTimeMs = random.nextLong();

    result.setId(id);
    result.setAddress(address);
    result.setLastContactSec(lastContactSec);
    result.setState(state);
    result.setCapacityBytes(capacityBytes);
    result.setUsedBytes(usedBytes);
    result.setStartTimeMs(startTimeMs);

    return result;
  }
}
