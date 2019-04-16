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
import alluxio.grpc.GrpcUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class WorkerNetAddressTest {

  @Test
  public void json() throws Exception {
    WorkerNetAddress workerNetAddress = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    WorkerNetAddress other =
        mapper.readValue(mapper.writeValueAsBytes(workerNetAddress), WorkerNetAddress.class);
    checkEquality(workerNetAddress, other);
  }

  @Test
  public void proto() {
    WorkerNetAddress workerNetAddress = createRandom();
    WorkerNetAddress other = GrpcUtils.fromProto(GrpcUtils.toProto(workerNetAddress));
    checkEquality(workerNetAddress, other);
  }

  public void checkEquality(WorkerNetAddress a, WorkerNetAddress b) {
    Assert.assertEquals(a.getHost(), b.getHost());
    Assert.assertEquals(a.getRpcPort(), b.getRpcPort());
    Assert.assertEquals(a.getDataPort(), b.getDataPort());
    Assert.assertEquals(a.getWebPort(), b.getWebPort());
    Assert.assertEquals(a.getTieredIdentity(), b.getTieredIdentity());
    Assert.assertEquals(a, b);
  }

  public static WorkerNetAddress createRandom() {
    WorkerNetAddress result = new WorkerNetAddress();
    Random random = new Random();

    String host = CommonUtils.randomAlphaNumString(random.nextInt(10));
    int rpcPort = random.nextInt();
    int dataPort = random.nextInt();
    int webPort = random.nextInt();
    TieredIdentity identity = TieredIdentityTest.createRandomTieredIdentity();

    result.setHost(host);
    result.setRpcPort(rpcPort);
    result.setDataPort(dataPort);
    result.setWebPort(webPort);
    result.setTieredIdentity(identity);

    return result;
  }
}
