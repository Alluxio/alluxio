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

package alluxio.executor;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A handler to measure reject count.
 */
public class MeasurableRejectedExecutionHandler implements RejectedExecutionHandler {
  private AtomicLong mCounter = new AtomicLong(0L);

  private final RejectedExecutionHandler mHandler;

  /**
   * Constructs a wrapped {@link RejectedExecutionHandler} to measure rejected count.
   *
   * @param handler the rejected execution handler
   */
  public MeasurableRejectedExecutionHandler(RejectedExecutionHandler handler) {
    mHandler = handler;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    mCounter.incrementAndGet();
    mHandler.rejectedExecution(r, executor);
  }

  /**
   * @return the rejected count
   */
  public long getCount() {
    return mCounter.get();
  }
}
