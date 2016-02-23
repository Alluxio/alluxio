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
    long numLocations = random.nextInt(10);
    for (int i = 0; i < numLocations; i++) {
      locations.add(BlockLocationTest.createRandom());
    }

    result.setBlockId(blockId);
    result.setLength(length);
    result.setLocations(locations);

    return result;
  }
}
