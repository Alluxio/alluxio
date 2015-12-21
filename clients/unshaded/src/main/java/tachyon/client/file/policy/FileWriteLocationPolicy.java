/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file.policy;

import java.util.List;

import tachyon.annotation.PublicApi;
import tachyon.client.block.BlockWorkerInfo;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.options.OutStreamOptions;

/**
 * <p>
 * Interface for the location policy of which workers a file's block are written into. A location
 * policy instance is used only once per file write.
 * </p>
 *
 * <p>
 * The {@link FileOutStream} creates a new policy based on the
 * {@link FileWriteLocationPolicyOptions} passed in {@link OutStreamOptions}. And then the stream
 * calls the {@link #initialize(List, FileWriteLocationPolicyOptions)} to initialize the policy.
 * Then the stream calls {@link #getWorkerForNextBlock(List)} to decide which worker to write the
 * next block per block write.
 * </p>
 */
@PublicApi
public interface FileWriteLocationPolicy<T extends FileWriteLocationPolicyOptions> {

  /**
   * Initializes the location policy with the information of the active workers and the options.
   *
   * @param workerInfoList the list of active workers information
   * @param policyOptions the policy options for configuring the policy
   */
  public void initialize(List<BlockWorkerInfo> workerInfoList, T policyOptions);

  /**
   * Gets the worker's host name for the next block to write to.
   *
   * @param workerInfoList the info of the active workers
   * @param the size of the block in bytes
   * @return the host name of worker to write to
   */
  public String getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList, long blockSizeBytes);
}
