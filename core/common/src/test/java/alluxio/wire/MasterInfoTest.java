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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MasterInfoTest {

  @Test
  public void json() throws Exception {
    MasterInfo masterInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    MasterInfo other =
        mapper.readValue(mapper.writeValueAsBytes(masterInfo), MasterInfo.class);
    checkEquality(masterInfo, other);
  }

  @Test
  public void proto() {
    MasterInfo masterInfo = createRandom();
    MasterInfo other = MasterInfo.fromThrift(masterInfo.toThrift());
    checkEquality(masterInfo, other);
  }

  private void checkEquality(MasterInfo a, MasterInfo b) {
    Assert.assertEquals(a.getLeaderMasterAddress(), b.getLeaderMasterAddress());
    Assert.assertEquals(a.getMasterAddresses(), b.getMasterAddresses());
    Assert.assertEquals(a.getRpcPort(), b.getRpcPort());
    Assert.assertEquals(a.isSafeMode(), b.isSafeMode());
    Assert.assertEquals(a.getStartTimeMs(), b.getStartTimeMs());
    Assert.assertEquals(a.getUpTimeMs(), b.getUpTimeMs());
    Assert.assertEquals(a.getVersion(), b.getVersion());
    Assert.assertEquals(a.getWebPort(), b.getWebPort());
    Assert.assertEquals(a.getWorkerAddresses(), b.getWorkerAddresses());
    Assert.assertEquals(a.getZookeeperAddresses(), b.getZookeeperAddresses());
    Assert.assertEquals(a, b);
  }

  private static MasterInfo createRandom() {
    MasterInfo result = new MasterInfo();
    Random random = new Random();

    String leaderMasterAddress = CommonUtils.randomAlphaNumString(random.nextInt(10));
    List<Address> masterAddresses = Arrays.asList(
        new Address(CommonUtils.randomAlphaNumString(random.nextInt(10)),
            random.nextInt(10)),
        new Address(CommonUtils.randomAlphaNumString(random.nextInt(10)),
            random.nextInt(10)));
    int rpcPort = random.nextInt(2000);
    boolean safeMode = random.nextBoolean();
    long startTimeMs = random.nextLong();
    long uptimeMs = random.nextLong();
    String version = CommonUtils.randomAlphaNumString(random.nextInt(10));
    int webPort = random.nextInt(2000);
    List<Address> workerAddresses = Arrays.asList(
        new Address(CommonUtils.randomAlphaNumString(random.nextInt(10)),
            random.nextInt(10)),
        new Address(CommonUtils.randomAlphaNumString(random.nextInt(10)),
            random.nextInt(10)));

    List<String> zookeeperAddresses = Arrays.asList(
        CommonUtils.randomAlphaNumString(random.nextInt(10)),
        CommonUtils.randomAlphaNumString(random.nextInt(10)),
        CommonUtils.randomAlphaNumString(random.nextInt(10)));

    result.setLeaderMasterAddress(leaderMasterAddress);
    result.setMasterAddresses(masterAddresses);
    result.setRpcPort(rpcPort);
    result.setSafeMode(safeMode);
    result.setStartTimeMs(startTimeMs);
    result.setUpTimeMs(uptimeMs);
    result.setVersion(version);
    result.setWebPort(webPort);
    result.setWorkerAddresses(workerAddresses);
    result.setZookeeperAddresses(zookeeperAddresses);
    return result;
  }
}
