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

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class WorkerNetAddressTest {

  @Test
  public void jsonTest() throws Exception {
    WorkerNetAddress workerNetAddress = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    WorkerNetAddress other =
        mapper.readValue(mapper.writeValueAsBytes(workerNetAddress), WorkerNetAddress.class);
    checkEquality(workerNetAddress, other);
  }

  @Test
  public void thriftTest() {
    WorkerNetAddress workerNetAddress = createRandom();
    WorkerNetAddress other = ThriftUtils.fromThrift(ThriftUtils.toThrift(workerNetAddress));
    checkEquality(workerNetAddress, other);
  }

  public void checkEquality(WorkerNetAddress a, WorkerNetAddress b) {
    Assert.assertEquals(a.getHost(), b.getHost());
    Assert.assertEquals(a.getRpcPort(), b.getRpcPort());
    Assert.assertEquals(a.getDataPort(), b.getDataPort());
    Assert.assertEquals(a.getWebPort(), b.getWebPort());
    Assert.assertEquals(a, b);
  }

  public static WorkerNetAddress createRandom() {
    WorkerNetAddress result = new WorkerNetAddress();
    Random random = new Random();

    String host = "";
    int hostLength = random.nextInt(10);
    for (int i = 0; i < hostLength; i++) {
      host += random.nextInt(96) + 32; // generates a random alphanumeric symbol
    }
    int rpcPort = random.nextInt();
    int dataPort = random.nextInt();
    int webPort = random.nextInt();

    result.setHost(host);
    result.setRpcPort(rpcPort);
    result.setDataPort(dataPort);
    result.setWebPort(webPort);

    return result;
  }
}
