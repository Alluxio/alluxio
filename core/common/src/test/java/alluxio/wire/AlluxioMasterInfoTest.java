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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class AlluxioMasterInfoTest {

  @Test
  public void json() throws Exception {
    AlluxioMasterInfo alluxioMasterInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    AlluxioMasterInfo other =
        mapper.readValue(mapper.writeValueAsBytes(alluxioMasterInfo), AlluxioMasterInfo.class);
    checkEquality(alluxioMasterInfo, other);
  }

  @Test
  public void equals() {
    alluxio.test.util.CommonUtils.testEquals(AlluxioMasterInfo.class);
  }

  private void checkEquality(AlluxioMasterInfo a, AlluxioMasterInfo b) {
    assertEquals(a.getCapacity(), b.getCapacity());
    assertEquals(a.getConfiguration(), b.getConfiguration());
    assertEquals(a.getLostWorkers(), b.getLostWorkers());
    assertEquals(a.getMetrics(), b.getMetrics());
    assertEquals(a.getRpcAddress(), b.getRpcAddress());
    assertEquals(a.getStartTimeMs(), b.getStartTimeMs());
    assertEquals(a.getTierCapacity(), b.getTierCapacity());
    assertEquals(a.getUfsCapacity(), b.getUfsCapacity());
    assertEquals(a.getUptimeMs(), b.getUptimeMs());
    assertEquals(a.getVersion(), b.getVersion());
    assertEquals(a.getWorkers(), b.getWorkers());
    assertEquals(a, b);
  }

  private static AlluxioMasterInfo createRandom() {
    AlluxioMasterInfo result = new AlluxioMasterInfo();
    Random random = new Random();

    Capacity capacity = CapacityTest.createRandom();
    Map<String, String> configuration = new HashMap<>();
    long numConfiguration = random.nextInt(10);
    for (int i = 0; i < numConfiguration; i++) {
      configuration.put(CommonUtils.randomAlphaNumString(random.nextInt(10)),
          CommonUtils.randomAlphaNumString(random.nextInt(10)));
    }
    List<WorkerInfo> lostWorkers = new ArrayList<>();
    long numLostWorkers = random.nextInt(10);
    for (int i = 0; i < numLostWorkers; i++) {
      lostWorkers.add(WorkerInfoTest.createRandom());
    }
    Map<String, Long> metrics = new HashMap<>();
    long numMetrics = random.nextInt(10);
    for (int i = 0; i < numMetrics; i++) {
      metrics.put(CommonUtils.randomAlphaNumString(random.nextInt(10)), random.nextLong());
    }
    String rpcAddress = CommonUtils.randomAlphaNumString(random.nextInt(10));
    long startTimeMs = random.nextLong();
    Map<String, Capacity> tierCapacity = new HashMap<>();
    long numTiers = random.nextInt(10);
    for (int i = 0; i < numTiers; i++) {
      tierCapacity
          .put(CommonUtils.randomAlphaNumString(random.nextInt(10)), CapacityTest.createRandom());
    }
    Capacity ufsCapacity = CapacityTest.createRandom();
    long uptimeMs = random.nextLong();
    String version = CommonUtils.randomAlphaNumString(random.nextInt(10));
    List<WorkerInfo> workers = new ArrayList<>();
    long numWorkers = random.nextInt(10);
    for (int i = 0; i < numWorkers; i++) {
      workers.add(WorkerInfoTest.createRandom());
    }

    result.setCapacity(capacity);
    result.setConfiguration(configuration);
    result.setLostWorkers(lostWorkers);
    result.setMetrics(metrics);
    result.setRpcAddress(rpcAddress);
    result.setStartTimeMs(startTimeMs);
    result.setTierCapacity(tierCapacity);
    result.setUfsCapacity(ufsCapacity);
    result.setUptimeMs(uptimeMs);
    result.setVersion(version);
    result.setWorkers(workers);

    return result;
  }
}
