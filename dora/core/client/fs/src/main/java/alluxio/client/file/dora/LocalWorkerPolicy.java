package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.Configuration;
import alluxio.util.network.NetworkAddressUtils;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class LocalWorkerPolicy implements WorkerLocationPolicy {

  @Override
  public List<BlockWorkerInfo> getPreferredWorkers(
      List<BlockWorkerInfo> blockWorkerInfos, String fileId, int count) {

    // TODO: ideally this conf should come from a context
    String userHostname = NetworkAddressUtils.getClientHostName(Configuration.global());

    BlockWorkerInfo localWorker = null;
    for (BlockWorkerInfo worker : blockWorkerInfos) {
      if (worker.getNetAddress().getHost().equals(userHostname)) {
        localWorker = worker;
        break;
      }
    }
    if (localWorker != null) {
      return ImmutableList.of(localWorker);
    } else {
      return ImmutableList.of();
    }
  }
}
