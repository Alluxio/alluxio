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

package alluxio.csi.client;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Convenience class to pass around Csi client options.
 */
@ThreadSafe
public final class CsiClientOptions {
  private final String mOperation;
  private final String mTarget;
  private final String mVolumeId;

  /**
   * @param operation the csi operation
   * @param target the target path
   * @param volumeId the volume id
   */
  public CsiClientOptions(String operation, String target, String volumeId) {
    mOperation = operation;
    mTarget = target;
    mVolumeId = volumeId;
  }

  /**
   * @return the csi operation
   */
  public String getOperation() {
    return mOperation;
  }

  /**
   * @return the target path
   */
  public String getTarget() {
    return mTarget;
  }

  /**
   * @return the volume id
   */
  public String getVolumeId() {
    return mVolumeId;
  }
}
