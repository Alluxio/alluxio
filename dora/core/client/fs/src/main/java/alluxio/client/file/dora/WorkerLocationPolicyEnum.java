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

package alluxio.client.file.dora;

/**
 * The enum of worker location policy.
 */
public enum WorkerLocationPolicyEnum {
  CONSISTENT("alluxio.client.file.dora.ConsistentHashPolicy"),
  JUMP("alluxio.client.file.dora.JumpHashPolicy"),
  KETAMA("alluxio.client.file.dora.KetamaHashPolicy"),
  MAGLEV("alluxio.client.file.dora.MaglevHashPolicy"),
  MULTI_PROBE("alluxio.client.file.dora.MultiProbeHashPolicy"),
  LOCAL("alluxio.client.file.dora.LocalWorkerPolicy"),
  REMOTE_ONLY("alluxio.client.file.dora.RemoteOnlyPolicy");

  private final String mPolicyName;

  WorkerLocationPolicyEnum(String policyName) {
    mPolicyName = policyName;
  }

  /**
   * @return the hash policy name
   */
  public String getPolicyName() {
    return mPolicyName;
  }
}
