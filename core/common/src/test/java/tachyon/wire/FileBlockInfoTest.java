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

package tachyon.wire;

import java.util.List;
import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

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
    long numUfsLocations = random.nextInt(10) + 1;
    List<WorkerNetAddress> ufsLocations = Lists.newArrayList();
    for (int i = 0; i < numUfsLocations; i ++) {
      ufsLocations.add(WorkerNetAddressTest.createRandom());
    }

    result.setBlockInfo(blockInfo);
    result.setOffset(offset);
    result.setUfsLocations(ufsLocations);

    return result;
  }
}
