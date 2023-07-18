package alluxio.client.file.dora;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

import java.util.List;

/**
 * Interface for determining the Alluxio worker location to serve a read or write request.
 *
 * A policy MUST have the property of being deterministic, because distributed clients must
 * be able to resolve the same path to the same worker(s) for the file.
 * Imagine a totally random policy which resolves a path to a random worker in the cluster.
 * The first reader of path /a will resolve to a random worker in the cluster and leave a cache
 * there. A subsequent reader will resolve to another random worker and the chance of a cache-hit
 * is very low! Worse still, that subsequent reader may produce another cache replica.
 * This random policy wastes cache space and provides terrible cache hit rate.
 */
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
    public static WorkerLocationPolicy create(AlluxioConfiguration conf) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.getClass(PropertyKey.USER_WORKER_SELECTION_POLICY),
            new Class[] {AlluxioConfiguration.class}, new Object[] {conf});
      } catch (ClassCastException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
