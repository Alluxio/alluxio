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

import alluxio.wire.WorkerInfo;

import java.util.Collection;

/**
 * The worker assign policy.
 * This policy is membership agnostic for now, it will only pick a worker
 * from a given set of workers based on the policy, it is caller's responsibility
 * to resolve any membership considerations before giving the worker set to pick from.
 */
public abstract class WorkerAssignPolicy {

  /**
   * Pick a worker based on the policy.
   * @param object object
   * @param workerInfos worker information
   * @return the picked worker
   */
  protected abstract WorkerInfo pickAWorker(String object, Collection<WorkerInfo> workerInfos);
}
