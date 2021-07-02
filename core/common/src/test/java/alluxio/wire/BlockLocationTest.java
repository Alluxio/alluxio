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
import alluxio.grpc.GrpcUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.Random;

public final class BlockLocationTest {

  @Test
  public void json() throws Exception {
    BlockLocation blockLocation = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    BlockLocation other =
        mapper.readValue(mapper.writeValueAsBytes(blockLocation), BlockLocation.class);
    checkEquality(blockLocation, other);
  }

  @Test
  public void proto() {
    BlockLocation blockLocation = createRandom();
    BlockLocation other = GrpcUtils.fromProto(GrpcUtils.toProto(blockLocation));
    checkEquality(blockLocation, other);
  }

  public void checkEquality(BlockLocation a, BlockLocation b) {
    assertEquals(a.getWorkerId(), b.getWorkerId());
    assertEquals(a.getWorkerAddress(), b.getWorkerAddress());
    assertEquals(a.getTierAlias(), b.getTierAlias());
    assertEquals(a, b);
  }

  public static BlockLocation createRandom() {
    BlockLocation result = new BlockLocation();
    Random random = new Random();

    long workerId = random.nextLong();
    WorkerNetAddress workerAddress = WorkerNetAddressTest.createRandom();
    String tierAlias = CommonUtils.randomAlphaNumString(random.nextInt(10));
    String mediumType = CommonUtils.randomAlphaNumString(random.nextInt(3));

    result.setWorkerId(workerId);
    result.setWorkerAddress(workerAddress);
    result.setTierAlias(tierAlias);
    result.setMediumType(mediumType);

    return result;
  }
}
