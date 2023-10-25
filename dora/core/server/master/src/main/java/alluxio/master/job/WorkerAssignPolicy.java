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

package alluxio.master.job;

import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.wire.WorkerInfo;

import java.util.List;
import java.util.Set;

/**
 * The worker assign policy.
 * This policy is membership agnostic for now, it will only pick a worker
 * from a given set of workers based on the policy, it is caller's responsibility
 * to resolve any membership considerations before giving the worker set to pick from.
 */
public interface WorkerAssignPolicy {

  /**
   * Pick workers based on the policy. If the expected number is not met, it will return empty list
   *
   * @param object      object
   * @param workerInfos distinct workers' information
   * @param count       expected number of distinct workers
   * @return the picked workers
   * @throws ResourceExhaustedRuntimeException if there are not enough workers to pick from
   */
  List<WorkerInfo> pickWorkers(String object, Set<WorkerInfo> workerInfos, int count);
}
