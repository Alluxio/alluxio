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
  public void thrift() {
    MasterInfo masterInfo = createRandom();
    MasterInfo other = MasterInfo.fromThrift(masterInfo.toThrift());
    checkEquality(masterInfo, other);
  }

  private void checkEquality(MasterInfo a, MasterInfo b) {
    Assert.assertEquals(a.getMasterAddress(), b.getMasterAddress());
    Assert.assertEquals(a.getRpcPort(), b.getRpcPort());
    Assert.assertEquals(a.isSafeMode(), b.isSafeMode());
    Assert.assertEquals(a.getStartTimeMs(), b.getStartTimeMs());
    Assert.assertEquals(a.getUpTimeMs(), b.getUpTimeMs());
    Assert.assertEquals(a.getVersion(), b.getVersion());
    Assert.assertEquals(a.getWebPort(), b.getWebPort());
    Assert.assertEquals(a.getZookeeperAddresses(), b.getZookeeperAddresses());
    Assert.assertEquals(a, b);
  }

  private static MasterInfo createRandom() {
    MasterInfo result = new MasterInfo();
    Random random = new Random();

    String masterAddress = CommonUtils.randomAlphaNumString(random.nextInt(10));
    int rpcPort = random.nextInt(2000);
    long startTimeMs = random.nextLong();
    long uptimeMs = random.nextLong();
    String version = CommonUtils.randomAlphaNumString(random.nextInt(10));
    int webPort = random.nextInt(2000);
    boolean safeMode = random.nextBoolean();
    List<String> zookeeperAddresses = Arrays.asList(
        CommonUtils.randomAlphaNumString(random.nextInt(10)),
        CommonUtils.randomAlphaNumString(random.nextInt(10)),
        CommonUtils.randomAlphaNumString(random.nextInt(10)));

    result.setMasterAddress(masterAddress);
    result.setRpcPort(rpcPort);
    result.setSafeMode(safeMode);
    result.setStartTimeMs(startTimeMs);
    result.setUpTimeMs(uptimeMs);
    result.setVersion(version);
    result.setWebPort(webPort);
    result.setZookeeperAddresses(zookeeperAddresses);
    return result;
  }
}
