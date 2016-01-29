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
import tachyon.WorkerNetAddress;

/**
 * <p>
 * Interface for the location policy of which workers a file's blocks are written into. A location
 * policy instance is used only once per file write.
 * </p>
 *
 * <p>
 * The {@link FileOutStream} calls {@link #getWorkerForNextBlock(List, long)} to decide which worker
 * to write the next block per block write.
 * </p>
 *
 * <p>
 * A policy must have an empty constructor to be used as default policy.
 * </p>
 */
@PublicApi
public interface FileWriteLocationPolicy {
  /**
   * Gets the worker's host name for the next block to write to.
   *
   * @param workerInfoList the info of the active workers
   * @param blockSizeBytes the size of the block in bytes
   * @return the address of the worker to write to
   */
  public WorkerNetAddress getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList,
                                                long blockSizeBytes);
}
