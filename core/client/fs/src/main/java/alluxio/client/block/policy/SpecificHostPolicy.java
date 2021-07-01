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

package alluxio.client.block.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Always returns a worker with the hostname specified by
 * {@link PropertyKey.WORKER_HOSTNAME} (alluxio.worker.hostname).
 */
@ThreadSafe
public final class SpecificHostPolicy implements BlockLocationPolicy {
  private final String mHostname;

  /**
   * Constructs a new {@link SpecificHostPolicy}.
   *
   * @param conf Alluxio configuration
   */
  public SpecificHostPolicy(AlluxioConfiguration conf) {
    this(conf.get(PropertyKey.WORKER_HOSTNAME));
  }

  /**
   * Constructs the policy with the hostname.
   *
   * @param hostname the name of the host
   */
  public SpecificHostPolicy(String hostname) {
    mHostname = Preconditions.checkNotNull(hostname, "hostname");
  }

  /**
   * Returns null if no active worker matches the hostname
   * provided in WORKER_HOSTNAME (alluxio.worker.hostname).
   */
  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    // find the first worker matching the host name
    for (BlockWorkerInfo info : options.getBlockWorkerInfos()) {
      if (info.getNetAddress().getHost().equals(mHostname)) {
        return info.getNetAddress();
      }
    }
    return null;
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
    return MoreObjects.toStringHelper(this)
        .add("hostname", mHostname)
        .toString();
  }
}
