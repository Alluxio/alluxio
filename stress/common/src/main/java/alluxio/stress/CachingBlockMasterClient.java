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

import alluxio.grpc.LocationBlockIdListEntry;
import alluxio.master.MasterClientContext;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockStoreLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Use this class to avoid the map-list conversion at the client side.
 * This reduces the client side work so the time measurement for the whole RPC better reflects
 * the time taken at the master side.
 * Also reducing the workload on the client side will avoid the test becoming CPU-bound on the
 * client side, when the concurrency is high.
 */
public class CachingBlockMasterClient extends BlockMasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(CachingBlockMasterClient.class);

  private final List<LocationBlockIdListEntry> mLocationBlockIdList;

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

  @Override
  public List<LocationBlockIdListEntry> convertBlockListMapToProto(
          Map<BlockStoreLocation, List<Long>> blockListOnLocation) {
    LOG.debug("Using the cached block list proto");
    return mLocationBlockIdList;
  }
}
