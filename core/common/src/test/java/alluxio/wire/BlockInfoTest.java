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

import java.util.List;
import java.util.Random;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BlockInfoTest {

  @Test
  public void jsonTest() throws Exception {
    BlockInfo blockInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    BlockInfo other = mapper.readValue(mapper.writeValueAsBytes(blockInfo), BlockInfo.class);
    checkEquality(blockInfo, other);
  }

  @Test
  public void thriftTest() {
    BlockInfo blockInfo = createRandom();
    BlockInfo other = ThriftUtils.fromThrift(ThriftUtils.toThrift(blockInfo));
    checkEquality(blockInfo, other);
  }

  public void checkEquality(BlockInfo a, BlockInfo b) {
    Assert.assertEquals(a.getBlockId(), b.getBlockId());
    Assert.assertEquals(a.getLength(), b.getLength());
    Assert.assertEquals(a.getLocations(), b.getLocations());
    Assert.assertEquals(a, b);
  }

  public static BlockInfo createRandom() {
    BlockInfo result = new BlockInfo();
    Random random = new Random();

    long blockId = random.nextLong();
    long length = random.nextLong();
    List<BlockLocation> locations = Lists.newArrayList();
    long numElements = random.nextInt(10) + 1;
    for (int i = 0; i < numElements; i++) {
      locations.add(BlockLocationTest.createRandom());
    }

    result.setBlockId(blockId);
    result.setLength(length);
    result.setLocations(locations);

    return result;
  }
}
