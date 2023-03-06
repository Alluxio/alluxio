package alluxio.scheduler.job;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.util.List;

/**
 * Interface for providing worker information and client.
 */
public interface WorkerProvider {

  /**
   * Gets a list of worker information.
   *
   * @return a list of worker information
   */
  List<WorkerInfo> getWorkerInfos();

  /**
   * Gets a worker client.
   *
   * @param address the worker address
   * @return a worker client
   */
  CloseableResource<BlockWorkerClient> getWorkerClient(WorkerNetAddress address);
}
