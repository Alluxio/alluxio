/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;

import java.util.concurrent.TimeoutException;

public class ChannelHealthChecker {
  private final ManagedChannel mManagedChannel;
  private final int mHealthCheckTimeoutMs;

  public ChannelHealthChecker(ManagedChannel managedChannel, int healthCheckTimeoutMs) {
    mManagedChannel = managedChannel;
    mHealthCheckTimeoutMs = healthCheckTimeoutMs;
  }

  public boolean waitForChannelReady() {
    try {
      Boolean res = CommonUtils.waitForResult("channel to be ready", () -> {
        ConnectivityState currentState = mManagedChannel.getState(true);
        switch (currentState) {
          case READY:
            return true;
          case TRANSIENT_FAILURE:
          case SHUTDOWN:
            return false;
          case IDLE:
          case CONNECTING:
            return null;
          default:
            return null;
        }
      }, WaitForOptions.defaults().setTimeoutMs((int) mHealthCheckTimeoutMs));
      return res;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }
}
