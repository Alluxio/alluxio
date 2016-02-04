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

package tachyon.wire;

import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class WorkerInfoTest {

  @Test
  public void jsonTest() throws Exception {
    WorkerInfo workerInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    WorkerInfo other =
        mapper.readValue(mapper.writeValueAsBytes(workerInfo), WorkerInfo.class);
    checkEquality(workerInfo, other);
  }

  @Test
  public void thriftTest() {
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
    byte[] bytes = new byte[5];
    random.nextBytes(bytes);
    String state = new String(bytes);
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
