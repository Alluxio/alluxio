package alluxio.stress.job;

import java.util.List;

public class RegisterWorkerBenchConfig extends StressBenchConfig {
  public long mStartId;
  public String mTiers;

  /**
   * @param className    the class name of the benchmark to run
   * @param args         the args for the benchmark
   * @param startDelayMs the start delay for the distributed tasks, in ms
   * @param clusterLimit the max number of workers to run on. If 0, run on entire cluster,
   */
  public RegisterWorkerBenchConfig(String className, List<String> args, long startDelayMs, int clusterLimit,
                        long start, String tiers) {
    super(className, args, startDelayMs, clusterLimit);

    mStartId = start;
    mTiers = tiers;
  }

}
