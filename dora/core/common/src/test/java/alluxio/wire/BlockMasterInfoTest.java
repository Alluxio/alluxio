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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public final class BlockMasterInfoTest {

  @Test
  public void json() throws Exception {
    BlockMasterInfo blockMasterInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    BlockMasterInfo other = mapper.readValue(mapper.writeValueAsBytes(blockMasterInfo),
        BlockMasterInfo.class);
    checkEquality(blockMasterInfo, other);
  }

  @Test
  public void proto() {
    BlockMasterInfo blockMasterInfo = createRandom();
    BlockMasterInfo other = BlockMasterInfo.fromProto(blockMasterInfo.toProto());
    checkEquality(blockMasterInfo, other);
  }

  public void checkEquality(BlockMasterInfo a, BlockMasterInfo b) {
    Assert.assertEquals(a.getCapacityBytes(), b.getCapacityBytes());
    Assert.assertEquals(a.getCapacityBytesOnTiers(), b.getCapacityBytesOnTiers());
    Assert.assertEquals(a.getFreeBytes(), b.getFreeBytes());
    Assert.assertEquals(a.getLiveWorkerNum(), b.getLiveWorkerNum());
    Assert.assertEquals(a.getLostWorkerNum(), b.getLostWorkerNum());
    Assert.assertEquals(a.getUsedBytes(), b.getUsedBytes());
    Assert.assertEquals(a.getUsedBytesOnTiers(), b.getUsedBytesOnTiers());
    Assert.assertEquals(a, b);
  }

  public static BlockMasterInfo createRandom() {
    BlockMasterInfo result = new BlockMasterInfo();
    Random random = new Random();

    long capacityBytes = random.nextLong();
    Map<String, Long> capacityBytesOnTiers = new HashMap<>();
    capacityBytesOnTiers.put(Constants.MEDIUM_MEM, capacityBytes);
    long freeBytes = random.nextLong();
    int liveWorkerNum = random.nextInt(10);
    int lostWorkerNum = random.nextInt(10);
    long usedBytes = random.nextLong();
    Map<String, Long> usedBytesOnTiers = new HashMap<>();
    usedBytesOnTiers.put(Constants.MEDIUM_MEM, capacityBytes);

    result.setCapacityBytes(capacityBytes);
    result.setCapacityBytesOnTiers(capacityBytesOnTiers);
    result.setFreeBytes(freeBytes);
    result.setLiveWorkerNum(liveWorkerNum);
    result.setLostWorkerNum(lostWorkerNum);
    result.setUsedBytes(usedBytes);
    result.setUsedBytesOnTiers(usedBytesOnTiers);

    return result;
  }
}
