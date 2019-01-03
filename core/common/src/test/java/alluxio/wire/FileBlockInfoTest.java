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
import alluxio.util.CommonUtils;
import alluxio.grpc.GrpcUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class FileBlockInfoTest {

  @Test
  public void json() throws Exception {
    FileBlockInfo fileBlockInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    FileBlockInfo other =
        mapper.readValue(mapper.writeValueAsBytes(fileBlockInfo), FileBlockInfo.class);
    checkEquality(fileBlockInfo, other);
  }

  @Test
  public void proto() {
    FileBlockInfo fileBlockInfo = createRandom();
    FileBlockInfo other = GrpcUtils.fromProto(GrpcUtils.toProto(fileBlockInfo));
    checkEquality(fileBlockInfo, other);
  }

  public void checkEquality(FileBlockInfo a, FileBlockInfo b) {
    Assert.assertEquals(a.getBlockInfo(), b.getBlockInfo());
    Assert.assertEquals(a.getOffset(), b.getOffset());
    Assert.assertEquals(a.getUfsLocations(), b.getUfsLocations());
    Assert.assertEquals(a, b);
  }

  public static FileBlockInfo createRandom() {
    FileBlockInfo result = new FileBlockInfo();
    Random random = new Random();

    BlockInfo blockInfo = BlockInfoTest.createRandom();
    long offset = random.nextLong();
    List<String> ufsLocations = new ArrayList<>();
    long numUfsLocations = random.nextInt(10);
    for (int i = 0; i < numUfsLocations; i++) {
      ufsLocations.add(HostAndPort.fromParts(CommonUtils.randomAlphaNumString(random.nextInt(10)),
          random.nextInt(Constants.MAX_PORT)).toString());
    }

    result.setBlockInfo(blockInfo);
    result.setOffset(offset);
    result.setUfsLocations(ufsLocations);

    return result;
  }
}
