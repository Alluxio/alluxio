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
   */
  public CachingBlockMasterClient(MasterClientContext conf, List<LocationBlockIdListEntry> locationBlockIdList) {
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
