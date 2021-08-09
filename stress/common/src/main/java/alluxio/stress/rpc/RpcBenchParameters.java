package alluxio.stress.rpc;

import alluxio.stress.Parameters;
import com.beust.jcommander.Parameter;

/**
 * Parameters for the RPC benchmark test.
 */
public class RpcBenchParameters extends Parameters {
  @Parameter(names = {"--concurrency"},
      description = "simulate this many clients/workers on one machine")
  public int mConcurrency = 2;

  @Parameter(names = {"--duration"},
      description = "The length of time to run the benchmark. (1m, 10m, 60s, 10000ms, etc.)")
  public String mDuration = "30s";
}
