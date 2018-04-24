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

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Always returns a worker with the specified hostname. Returns null if no active worker on that
 * hostname found.
 */
// TODO(peis): Move the BlockLocationPolicy implementation to alluxio.client.block.policy.
@ThreadSafe
public final class SpecificHostPolicy implements FileWriteLocationPolicy, BlockLocationPolicy {
  private final String mHostname;

  /**
   * Constructs the policy with the hostname.
   *
   * @param hostname the name of the host
   */
  public SpecificHostPolicy(String hostname) {
    mHostname = Preconditions.checkNotNull(hostname, "hostname");
  }

  @Override
  public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    // find the first worker matching the host name
    for (BlockWorkerInfo info : workerInfoList) {
      if (info.getNetAddress().getHost().equals(mHostname)) {
        return info.getNetAddress();
      }
    }
    return null;
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    return getWorkerForNextBlock(options.getBlockWorkerInfos(), options.getBlockSize());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SpecificHostPolicy)) {
      return false;
    }
    SpecificHostPolicy that = (SpecificHostPolicy) o;
    return Objects.equal(mHostname, that.mHostname);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mHostname);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("hostname", mHostname)
        .toString();
  }
}
