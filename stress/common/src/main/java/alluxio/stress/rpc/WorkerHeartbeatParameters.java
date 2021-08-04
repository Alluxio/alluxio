package alluxio.stress.rpc;

import com.beust.jcommander.Parameter;

public class WorkerHeartbeatParameters extends RpcBenchParameters {
  @Parameter(names = {"--fake-same-worker"},
          description = "For each worker heartbeat RPC, should this be same worker or different worker.")
  public boolean mSameWorker = false;

  @Parameter(names = {"--tiers"})
  /**
   * Examples: "100,200,300;1000,1500;2000"
   * Use semi-colon to separate tiers, use commas to separate dirs
   * */
  public String mTiers;
}
