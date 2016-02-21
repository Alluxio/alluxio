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

public class BlockLocationTest {

  @Test
  public void jsonTest() throws Exception {
    BlockLocation blockLocation = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    BlockLocation other =
        mapper.readValue(mapper.writeValueAsBytes(blockLocation), BlockLocation.class);
    checkEquality(blockLocation, other);
  }

  @Test
  public void thriftTest() {
    BlockLocation blockLocation = createRandom();
    BlockLocation other = ThriftUtils.fromThrift(ThriftUtils.toThrift(blockLocation));
    checkEquality(blockLocation, other);
  }

  public void checkEquality(BlockLocation a, BlockLocation b) {
    Assert.assertEquals(a.getWorkerId(), b.getWorkerId());
    Assert.assertEquals(a.getWorkerAddress(), b.getWorkerAddress());
    Assert.assertEquals(a.getTierAlias(), b.getTierAlias());
    Assert.assertEquals(a, b);

  }

  public static BlockLocation createRandom() {
    BlockLocation result = new BlockLocation();
    Random random = new Random();

    long workerId = random.nextLong();
    WorkerNetAddress workerAddress = WorkerNetAddressTest.createRandom();
    String tierAlias = CommonUtils.randomString(random.nextInt(10));

    result.setWorkerId(workerId);
    result.setWorkerAddress(workerAddress);
    result.setTierAlias(tierAlias);

    return result;
  }
}
