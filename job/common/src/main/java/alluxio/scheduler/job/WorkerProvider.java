/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.scheduler.job;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.exception.runtime.AlluxioRuntimeException;
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
   * @throws AlluxioRuntimeException if failed to get worker information
   */
  List<WorkerInfo> getWorkerInfos();

  /**
   * Gets a worker client.
   *
   * @param address the worker address
   * @return a worker client
   * @throws AlluxioRuntimeException if failed to get worker client
   */
  CloseableResource<BlockWorkerClient> getWorkerClient(WorkerNetAddress address);
}
