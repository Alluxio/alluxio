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

package alluxio.heartbeat;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An interface for a heartbeat execution. The {@link HeartbeatThread} calls the
 * {@link #heartbeat()} method.
 */
public interface HeartbeatExecutor extends Closeable {
  /**
   * Implements the heartbeat logic.
   *
   * @throws InterruptedException if the thread is interrupted
   */
  void heartbeat() throws InterruptedException;

  /**
   * Sets the tracker which will be set to {@code true} when shutdown
   * is initiated for the underlying thread pool that runs this heartbeat.
   *
   * @param shutdownTracker shutdown tracker reference
   */
  void setShutdownTracker(AtomicBoolean shutdownTracker);

  /**
   * Cleans up any resources used by the heartbeat executor.
   */
  @Override
  void close();
}
