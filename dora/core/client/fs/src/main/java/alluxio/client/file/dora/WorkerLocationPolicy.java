package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import com.google.common.collect.ImmutableList;

import java.util.List;

public interface WorkerLocationPolicy {
  /**
   * TODO(jiacheng): the semantics here is ambiguous:
   *  If I want count=3, what does it mean if the policy returns a list of 2 or empty?
   *
   * @param blockWorkerInfos
   * @param fileId
   * @param count
   * @return a list of preferred workers
   */
  List<BlockWorkerInfo> getPreferredWorkers(
      List<BlockWorkerInfo> blockWorkerInfos, String fileId, int count);

  /**
   * The factory for the {@link BlockLocationPolicy}.
   */
  class Factory {
    private Factory() {} // prevent instantiation

    /**
     * Factory for creating {@link BlockLocationPolicy}.
     *
     * @param conf Alluxio configuration
     * @return a new instance of {@link BlockLocationPolicy}
     */
    // TODO(jiacheng): use enum
    public static WorkerLocationPolicy create(AlluxioConfiguration conf) {
      String policyName = conf.getString(PropertyKey.USER_WORKER_SELECTION_POLICY);
      if (policyName.equals("HASH")) {
        return new ConsistentHashPolicy();
      } else if (policyName.equals("LOCAL")) {
        return new LocalWorkerPolicy();
      } else {
        throw new IllegalArgumentException("Policy " + policyName + " is unrecognized");
      }
    }
  }
}
