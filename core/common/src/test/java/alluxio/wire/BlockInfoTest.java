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

import alluxio.grpc.GrpcUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class BlockInfoTest {

  /**
   * Test to convert between a BlockInfo type and a json type.
   */
  @Test
  public void json() throws Exception {
    BlockInfo blockInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    BlockInfo other = mapper.readValue(mapper.writeValueAsBytes(blockInfo), BlockInfo.class);
    checkEquality(blockInfo, other);
  }

  /**
   * Test to convert between a proto type and a wire type.
   */
  @Test
  public void proto() {
    BlockInfo blockInfo = createRandom();
    BlockInfo other = GrpcUtils.fromProto(GrpcUtils.toProto(blockInfo));
    checkEquality(blockInfo, other);
  }

  /**
   * Check if the two BlockInfo object are equal.
   *
   * @param a the first BlockInfo object to be checked
   * @param b the second BlockInfo object to be checked
   */
  public void checkEquality(BlockInfo a, BlockInfo b) {
    assertEquals(a.getBlockId(), b.getBlockId());
    assertEquals(a.getLength(), b.getLength());
    assertEquals(a.getLocations(), b.getLocations());
    assertEquals(a, b);
  }

  /**
   * Randomly create a BlockInfo object.
   *
   * @return the created BlockInfo object
   */
  public static BlockInfo createRandom() {
    BlockInfo result = new BlockInfo();
    Random random = new Random();

    long blockId = random.nextLong();
    long length = random.nextLong();
    List<BlockLocation> locations = new ArrayList<>();
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
