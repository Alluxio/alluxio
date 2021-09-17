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

package alluxio.worker.block;

import static org.junit.Assert.assertEquals;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.BlockStoreLocationProto;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BlockMasterClientTest {
  private InstancedConfiguration mConf = ServerConfiguration.global();

  @Test
  public void convertBlockListMapToProtoMergeDirsInSameTier() {
    BlockMasterClient client = new BlockMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(mConf)).build());

    Map<BlockStoreLocation, List<Long>> blockMap = new HashMap<>();
    BlockStoreLocation memDir0 = new BlockStoreLocation("MEM", 0);
    blockMap.put(memDir0, Arrays.asList(1L, 2L, 3L));
    BlockStoreLocation memDir1 = new BlockStoreLocation("MEM", 1);
    blockMap.put(memDir1, Arrays.asList(4L, 5L, 6L, 7L));
    BlockStoreLocation ssdDir0 = new BlockStoreLocation("SSD", 0);
    blockMap.put(ssdDir0, Arrays.asList(11L, 12L, 13L, 14L));
    BlockStoreLocation ssdDir1 = new BlockStoreLocation("SSD", 1);
    blockMap.put(ssdDir1, Arrays.asList(15L, 16L, 17L, 18L, 19L));

    // Directories on the same tier will be merged together
    List<LocationBlockIdListEntry> protoList = client.convertBlockListMapToProto(blockMap);
    assertEquals(2, protoList.size());
    BlockStoreLocationProto memLocationProto = BlockStoreLocationProto.newBuilder()
        .setTierAlias("MEM").setMediumType("").build();
    BlockStoreLocationProto ssdLocationProto = BlockStoreLocationProto.newBuilder()
        .setTierAlias("SSD").setMediumType("").build();
    Set<BlockStoreLocationProto> blockLocations = protoList.stream()
        .map(LocationBlockIdListEntry::getKey).collect(Collectors.toSet());
    assertEquals(ImmutableSet.of(memLocationProto, ssdLocationProto), blockLocations);

    LocationBlockIdListEntry firstEntry = protoList.get(0);
    if (firstEntry.getKey().getTierAlias().equals("MEM")) {
      LocationBlockIdListEntry memTierEntry = protoList.get(0);
      List<Long> memProtoBlockList = memTierEntry.getValue().getBlockIdList();
      assertEquals(ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L, 7L),
              new HashSet<>(memProtoBlockList));
      LocationBlockIdListEntry ssdTierEntry = protoList.get(1);
      List<Long> ssdProtoBlockList = ssdTierEntry.getValue().getBlockIdList();
      assertEquals(ImmutableSet.of(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L),
              new HashSet<>(ssdProtoBlockList));
    } else {
      LocationBlockIdListEntry memTierEntry = protoList.get(1);
      List<Long> memProtoBlockList = memTierEntry.getValue().getBlockIdList();
      assertEquals(ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L, 7L),
              new HashSet<>(memProtoBlockList));
      LocationBlockIdListEntry ssdTierEntry = protoList.get(0);
      List<Long> ssdProtoBlockList = ssdTierEntry.getValue().getBlockIdList();
      assertEquals(ImmutableSet.of(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L),
              new HashSet<>(ssdProtoBlockList));
    }
  }
}
