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

import alluxio.Constants;
import alluxio.grpc.GrpcUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
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
  public void proto() {
    WorkerInfo workerInfo = createRandom();
    WorkerInfo other = GrpcUtils.fromProto(GrpcUtils.toProto(workerInfo));
    checkEquality(workerInfo, other);
  }

  @Test
  public void lastContactSecComparator() {
    Assert.assertTrue(compareLostWorkersWithTimes(0, 1) < 0);
    Assert.assertTrue(compareLostWorkersWithTimes(1, 0) > 0);
    Assert.assertTrue(compareLostWorkersWithTimes(1, 1) == 0);
    Assert.assertTrue(compareLostWorkersWithTimes(-1, 1) < 0);
    Assert.assertTrue(compareLostWorkersWithTimes(1, -1) > 0);
  }

  public void checkEquality(WorkerInfo a, WorkerInfo b) {
    Assert.assertEquals(a.getId(), b.getId());
    Assert.assertEquals(a.getAddress(), b.getAddress());
    Assert.assertEquals(a.getLastContactSec(), b.getLastContactSec());
    Assert.assertEquals(a.getCapacityBytes(), b.getCapacityBytes());
    Assert.assertEquals(a.getUsedBytes(), b.getUsedBytes());
    Assert.assertEquals(a.getStartTimeMs(), b.getStartTimeMs());
    Assert.assertEquals(a.getState(), b.getState());
    Assert.assertEquals(a.getCapacityBytesOnTiers(), b.getCapacityBytesOnTiers());
    Assert.assertEquals(a.getUsedBytesOnTiers(), b.getUsedBytesOnTiers());
    Assert.assertEquals(a, b);
  }

  private static int compareLostWorkersWithTimes(int time1, int time2) {
    WorkerInfo.LastContactSecComparator comparator =
        new WorkerInfo.LastContactSecComparator();
    WorkerInfo worker1 = createRandom();
    WorkerInfo worker2 = createRandom();
    worker1.setLastContactSec(time1);
    worker2.setLastContactSec(time2);
    return comparator.compare(worker1, worker2);
  }

  public static WorkerInfo createRandom() {
    WorkerInfo result = new WorkerInfo();
    Random random = new Random();

    long id = random.nextLong();
    WorkerNetAddress address = WorkerNetAddressTest.createRandom();
    int lastContactSec = random.nextInt();
    long capacityBytes = random.nextLong();
    long usedBytes = random.nextLong();
    long startTimeMs = random.nextLong();
    Map<String, Long> capacityBytesOnTiers = new HashMap<>();
    capacityBytesOnTiers.put(Constants.MEDIUM_MEM, capacityBytes);
    Map<String, Long> usedBytesOnTiers = new HashMap<>();
    usedBytesOnTiers.put(Constants.MEDIUM_MEM, usedBytes);
    String state = random.nextInt(1) == 1 ? "In Service" : "Out of Service";

    result.setId(id);
    result.setAddress(address);
    result.setLastContactSec(lastContactSec);
    result.setCapacityBytes(capacityBytes);
    result.setUsedBytes(usedBytes);
    result.setStartTimeMs(startTimeMs);
    result.setState(state);
    result.setCapacityBytesOnTiers(capacityBytesOnTiers);
    result.setUsedBytesOnTiers(usedBytesOnTiers);
    return result;
  }
}
