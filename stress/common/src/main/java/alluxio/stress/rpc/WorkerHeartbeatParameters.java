package alluxio.stress.rpc;

import com.beust.jcommander.Parameter;

public class WorkerHeartbeatParameters extends RpcBenchParameters {
  @Parameter(names = {"--fake-same-worker"},
          description = "For each registerWorker RPC, should this be same worker or different worker.")
  public boolean mSameWorker = false;

  @Parameter(names = {"--duration"},
          description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";

  @Parameter(names = {"--tiers"})
  /**
   * Examples: "100,200,300;1000,1500;2000"
   * Use semi-colon to separate tiers, use commas to separate dirs
   * */
  public String mTiers;
}
