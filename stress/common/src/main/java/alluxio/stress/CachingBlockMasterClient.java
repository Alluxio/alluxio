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
 * Use this class to avoid map-list conversion at the client side
 * So if you are running all processes on the same machine,
 * the client side work affect the performance less.
 * */
public class CachingBlockMasterClient extends BlockMasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(CachingBlockMasterClient.class);

  private List<LocationBlockIdListEntry> mLocationBlockIdList;

  /**
   * Creates a new instance of {@link BlockMasterClient} for the worker.
   *
   * @param conf master client configuration
   */
  public CachingBlockMasterClient(MasterClientContext conf, List<LocationBlockIdListEntry> locationBlockIdList) {
    super(conf);
    LOG.info("Init MockBlockMasterClient");
    mLocationBlockIdList = locationBlockIdList;
  }

  @Override
  public List<LocationBlockIdListEntry> convertBlockListMapToProto(
          Map<BlockStoreLocation, List<Long>> blockListOnLocation) {
    LOG.info("Using the prepared mLocationBlockIdList");
    return mLocationBlockIdList;
  }
}
