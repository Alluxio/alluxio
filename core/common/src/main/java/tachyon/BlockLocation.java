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

package tachyon;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The location of a block.
 */
public class BlockLocation {
  @JsonProperty("workerId")
  private long mWorkerId;
  @JsonProperty("address")
  private WorkerNetAddress mWorkerAddress;
  @JsonProperty("tierAlias")
  private String mTierAlias;

  /**
   * Creates a new instance of {@BlockLocation}.
   */
  public BlockLocation() {}

  /**
   * Creates a new instance of {@BlockLocation}.
   *
   * @param workerId the worker id to use
   * @param workerAddress the worker address to use
   * @param tierAlias the worker tier alias to use
   */
  public BlockLocation(long workerId, WorkerNetAddress workerAddress, String tierAlias) {
    mWorkerId = workerId;
    mWorkerAddress = workerAddress;
    mTierAlias = tierAlias;
  }

  /**
   * Creates a new instance of {@link BlockLocation} from a thrift representation.
   *
   * @param blockLocation the thrift representation of a block location
   */
  public BlockLocation(tachyon.thrift.BlockLocation blockLocation) {
    mWorkerId = blockLocation.getWorkerId();
    mWorkerAddress = new WorkerNetAddress(blockLocation.getWorkerAddress());
    mTierAlias = blockLocation.getTierAlias();
  }

  /**
   * @return the worker id
   */
  public long getWorkerId() {
    return mWorkerId;
  }

  /**
   * @return the worker address
   */
  public WorkerNetAddress getWorkerAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the tier alias
   */
  public String getTierAlias() {
    return mTierAlias;
  }

  /**
   * @param workerId the worker id to use
   */
  public void setWorkerId(long workerId) {
    mWorkerId = workerId;
  }

  /**
   * @param workerAddress the worker address to use
   */
  public void setWorkerAddress(WorkerNetAddress workerAddress) {
    mWorkerAddress = workerAddress;
  }

  /**
   * @param tierAlias the tier alias to use
   */
  public void setTierAlias(String tierAlias) {
    mTierAlias = tierAlias;
  }

  /**
   * @return thrift representation of the block location
   */
  public tachyon.thrift.BlockLocation toThrift() {
    return new tachyon.thrift.BlockLocation(mWorkerId, mWorkerAddress.toThrift(), mTierAlias);
  }
}
