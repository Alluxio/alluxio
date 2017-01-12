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

import alluxio.CommonTestUtils;
import alluxio.util.CommonUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class AlluxioProxyInfoTest {

  @Test
  public void json() throws Exception {
    AlluxioProxyInfo alluxioProxyInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    AlluxioProxyInfo other =
        mapper.readValue(mapper.writeValueAsBytes(alluxioProxyInfo), AlluxioProxyInfo.class);
    checkEquality(alluxioProxyInfo, other);
  }

  @Test
  public void equals() {
    CommonTestUtils.testEquals(AlluxioProxyInfo.class);
  }

  private void checkEquality(AlluxioProxyInfo a, AlluxioProxyInfo b) {
    Assert.assertEquals(a.getConfiguration(), b.getConfiguration());
    Assert.assertEquals(a.getStartTimeMs(), b.getStartTimeMs());
    Assert.assertEquals(a.getUptimeMs(), b.getUptimeMs());
    Assert.assertEquals(a.getVersion(), b.getVersion());
    Assert.assertEquals(a, b);
  }

  private static AlluxioProxyInfo createRandom() {
    AlluxioProxyInfo result = new AlluxioProxyInfo();
    Random random = new Random();

    Map<String, String> configuration = new HashMap<>();
    long numConfiguration = random.nextInt(10);
    for (int i = 0; i < numConfiguration; i++) {
      configuration.put(CommonUtils.randomAlphaNumString(random.nextInt(10)),
          CommonUtils.randomAlphaNumString(random.nextInt(10)));
    }
    long startTimeMs = random.nextLong();
    long uptimeMs = random.nextLong();
    String version = CommonUtils.randomAlphaNumString(random.nextInt(10));

    result.setConfiguration(configuration);
    result.setStartTimeMs(startTimeMs);
    result.setUptimeMs(uptimeMs);
    result.setVersion(version);

    return result;
  }
}
