/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.wire;

import com.google.common.collect.Lists;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class FileBlockInfoTest {

  @Test
  public void jsonTest() throws Exception {
    FileBlockInfo fileBlockInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    FileBlockInfo other =
        mapper.readValue(mapper.writeValueAsBytes(fileBlockInfo), FileBlockInfo.class);
    checkEquality(fileBlockInfo, other);
  }

  @Test
  public void thriftTest() {
    FileBlockInfo fileBlockInfo = createRandom();
    FileBlockInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(fileBlockInfo));
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
    List<WorkerNetAddress> ufsLocations = Lists.newArrayList();
    long numUfsLocations = random.nextInt(10);
    for (int i = 0; i < numUfsLocations; i++) {
      ufsLocations.add(WorkerNetAddressTest.createRandom());
    }

    result.setBlockInfo(blockInfo);
    result.setOffset(offset);
    result.setUfsLocations(ufsLocations);

    return result;
  }
}
