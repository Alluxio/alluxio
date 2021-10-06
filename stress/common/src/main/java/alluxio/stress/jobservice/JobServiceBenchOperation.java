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

package alluxio.stress.jobservice;

import alluxio.stress.Operation;
import alluxio.stress.master.MasterBenchOperation;

/**
 * The operations for the job service stress tests.
 */
public enum JobServiceBenchOperation implements Operation<JobServiceBenchOperation> {
  CREATE_FILES("CreateFiles"),
  DISTRIBUTED_LOAD("DistributedLoad"),
  NO_OP("NoOp");

  private final String mName;

  /**
   * @param name Name of the operation
   */
  JobServiceBenchOperation(String name) {
    mName = name;
  }


  @Override
  public String toString() {
    return mName;
  }


  @Override
  public JobServiceBenchOperation[] enumValues() {
    return JobServiceBenchOperation.values();
  }
}
