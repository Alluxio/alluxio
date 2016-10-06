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

import com.google.common.base.Throwables;
import org.powermock.reflect.Whitebox;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test utils for {@link RetryHandlingBlockWorkerClient}.
 */
public class RetryHandlingBlockWorkerClientTestUtils {
  /**
   * Resets the {@link RetryHandlingBlockWorkerClient#HEARTBEAT_CANCEL_POOL} by waiting for all the
   * pending heartbeats.
   */
  public static void reset() {
    while (true) {
      AtomicInteger numActiveHeartbeats =
          Whitebox.getInternalState(RetryHandlingBlockWorkerClient.class, "NUM_ACTIVE_HEARTBEATS");
      if (numActiveHeartbeats.intValue() > 0) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    }
  }
}
