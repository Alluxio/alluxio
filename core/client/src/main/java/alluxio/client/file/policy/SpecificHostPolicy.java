/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Always returns a worker with the specified hostname. Returns null if no active worker on that
 * hostname found.
 */
@ThreadSafe
public final class SpecificHostPolicy implements FileWriteLocationPolicy {
  private final String mHostname;

  /**
   * Constructs the policy with the hostname.
   *
   * @param hostname the name of the host
   */
  public SpecificHostPolicy(String hostname) {
    mHostname = Preconditions.checkNotNull(hostname);
  }

  @Override
  public WorkerNetAddress getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    // find the first worker matching the host name
    for (BlockWorkerInfo info : workerInfoList) {
      if (info.getNetAddress().getHost().equals(mHostname)) {
        return info.getNetAddress();
      }
    }
    return null;
  }
}
