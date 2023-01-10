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

import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.worker.block.BlockMapIterator;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.RegisterStreamer;

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
  public CachingBlockMapIterator mBlockBatchIterator;

  /**
   * Creates a new instance and caches the converted proto.
   *
   * @param conf master client configuration
   * @param locationBlockIdList location block id list
   */
  public CachingBlockMasterClient(MasterClientContext conf,
      List<LocationBlockIdListEntry> locationBlockIdList) {
    super(conf);
    LOG.debug("Init CachingBlockMasterClient for unary RPC");
    mLocationBlockIdList = locationBlockIdList;
  }

  /**
   * Creates a new instance and caches the converted proto.
   *
   * @param conf master client configuration
   * @param blockMap block lists of each location
   */
  public CachingBlockMasterClient(MasterClientContext conf,
      Map<BlockStoreLocation, List<Long>> blockMap) {
    super(conf);
    LOG.info("Init CachingBlockMasterClient for streaming RPC");
    mBlockBatchIterator = new CachingBlockMapIterator(blockMap, conf.getClusterConf());
  }

  @Override
  public List<LocationBlockIdListEntry> convertBlockListMapToProto(
          Map<BlockStoreLocation, List<Long>> blockListOnLocation) {
    LOG.debug("Using the cached block list proto");
    return mLocationBlockIdList;
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
        RegisterStreamer stream =
            new RegisterStreamer(mAsyncClient, workerId, storageTierAliases, totalBytesOnTiers,
                usedBytesOnTiers, lostStorage, configList, mBlockBatchIterator);
        stream.registerWithMaster();
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

  /**
   * Pre-generate the list so the RPC execution time does not include the conversion time.
   */
  public static class CachingBlockMapIterator extends BlockMapIterator {
    List<List<LocationBlockIdListEntry>> mBatches;
    Iterator<List<LocationBlockIdListEntry>> mDelegate;

    /**
     * Constructor.
     *
     * @param blockLocationMap the block lists of each location
     * @param conf configuration properties
     */
    public CachingBlockMapIterator(
        Map<BlockStoreLocation, List<Long>> blockLocationMap, AlluxioConfiguration conf) {
      super(blockLocationMap, conf);
      mBatches = new ArrayList<>();
      while (super.hasNext()) {
        mBatches.add(super.next());
      }
      mDelegate = mBatches.iterator();
    }

    @Override
    public boolean hasNext() {
      return mDelegate.hasNext();
    }

    @Override
    public List<LocationBlockIdListEntry> next() {
      return mDelegate.next();
    }
  }
}
