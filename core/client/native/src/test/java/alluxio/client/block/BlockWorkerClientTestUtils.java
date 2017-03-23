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

package alluxio.client.block;

import alluxio.Constants;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Function;
import org.powermock.reflect.Whitebox;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test utils for {@link BlockWorkerClient}.
 */
public class BlockWorkerClientTestUtils {
  /**
   * Resets the static {@link BlockWorkerClient} state.
   */
  public static void reset() {
    // reset the heartbeat pool by waiting for all the pending heartbeats
    CommonUtils
        .waitFor("All active block worker sessions are closed", new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            AtomicInteger numActiveHeartbeats = Whitebox
                .getInternalState(RetryHandlingBlockWorkerClient.class, "NUM_ACTIVE_SESSIONS");
            return numActiveHeartbeats.intValue() == 0;
          }
        }, WaitForOptions.defaults().setTimeout(Constants.MINUTE_MS));
  }
}
