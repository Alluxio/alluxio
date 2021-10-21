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

package alluxio.stress;

import alluxio.grpc.BlockIdList;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.worker.block.BlockMapIterator;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockStoreLocation;

import alluxio.worker.block.RegisterStreamer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Use this class to avoid the map-list conversion at the client side.
 * This reduces the client side work so the time measurement for the whole RPC better reflects
 * the time taken at the master side.
 * Also reducing the workload on the client side will avoid the test becoming CPU-bound on the
 * client side, when the concurrency is high.
 */
public class CachingBlockMasterClient extends BlockMasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(CachingBlockMasterClient.class);

  private List<LocationBlockIdListEntry> mLocationBlockIdList;
  public List<List<LocationBlockIdListEntry>> mBlockBatches;
  private Iterator<List<LocationBlockIdListEntry>> mBlockBatchIterator;

  /**
   * Creates a new instance and caches the converted proto.
   *
   * @param conf master client configuration
   * @param locationBlockIdList location block id list
   */
  public CachingBlockMasterClient(MasterClientContext conf,
                                  List<LocationBlockIdListEntry> locationBlockIdList) {
    super(conf);
    LOG.debug("Init CachingBlockMasterClient");
    mLocationBlockIdList = locationBlockIdList;
  }

  public CachingBlockMasterClient(MasterClientContext conf,
                                  Map<BlockStoreLocation, List<Long>> blockMap) {
    super(conf);
    LOG.info("Init CachingBlockMasterClient for streaming");
//    mLocationBlockIdList = locationBlockIdList;

    BlockMapIterator iter = new BlockMapIterator(blockMap, conf.getClusterConf());

    // Pre-generate the request batches
    mBlockBatches = ImmutableList.copyOf(iter);
    LOG.info("Prepared {} batches for requests", mBlockBatches.size());

    mBlockBatchIterator = mBlockBatches.iterator();
  }

  @Override
  public List<LocationBlockIdListEntry> convertBlockListMapToProto(
          Map<BlockStoreLocation, List<Long>> blockListOnLocation) {
    LOG.debug("Using the cached block list proto");
    return mLocationBlockIdList;
  }


  List<List<LocationBlockIdListEntry>> prepareBlockBatchesForStreaming() {
    // TODO(jiacheng): Avoid converting this back
    List<List<LocationBlockIdListEntry>> result = new ArrayList<>();
    for (LocationBlockIdListEntry entry : mLocationBlockIdList) {
      int len = entry.getValue().getBlockIdCount();
      // TODO(jiacheng): size configurable
      if (len <= 1000) {
        result.add(ImmutableList.of(entry));
        continue;
      }
      // Partition the list into multiple
      List<Long> list = entry.getValue().getBlockIdList();
      List<List<Long>> sublists = Lists.partition(list, 1000);
      LOG.info("Partitioned a LocationBlockIdListEntry of {} blocks into {} sublists", list.size(), sublists.size());
      // Regenerate the protos
      for (List<Long> sub : sublists) {
        List<LocationBlockIdListEntry> newList = new ArrayList<>();
        BlockIdList newBlockList = BlockIdList.newBuilder().addAllBlockId(sub).build();
        LocationBlockIdListEntry newEntry = LocationBlockIdListEntry.newBuilder()
                .setKey(entry.getKey()).setValue(newBlockList).build();
        newList.add(newEntry);
        result.add(newList);
      }
    }

    LOG.info("Partitioned a list of {} into {} sublists", mLocationBlockIdList.size(), result.size());
    return result;
  }

  @Override
  public void registerWithStream(final long workerId, final List<String> storageTierAliases,
                                 final Map<String, Long> totalBytesOnTiers, final Map<String, Long> usedBytesOnTiers,
                                 final Map<BlockStoreLocation, List<Long>> currentBlocksOnLocation,
                                 final Map<String, List<String>> lostStorage,
                                 final List<ConfigProperty> configList) throws IOException {
    AtomicReference<IOException> ioe = new AtomicReference<>();
    retryRPC(() -> {
      try {
        RegisterStreamer stream = new RegisterStreamer(mClient, mAsyncClient, workerId, storageTierAliases, totalBytesOnTiers, usedBytesOnTiers,
                currentBlocksOnLocation, lostStorage, configList, mBlockBatchIterator);
        stream.registerSync();
      } catch (IOException e) {
        ioe.set(e);
      } catch (InterruptedException e) {
        ioe.set(new IOException(e));
      }
      return null;
    }, LOG, "Register", "workerId=%d", workerId);

    if (ioe.get() != null) {
      throw ioe.get();
    }
  }
}
