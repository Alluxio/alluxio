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

package alluxio.util.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of the SerializedTaskRunner which simply executes the tasks as they are added
 * within the same thread. The tasks are never queued.
 */
public class SameThreadSerializedTaskRunner extends SerializedTaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SameThreadSerializedTaskRunner.class);

  /**
   * Create a new instance of {@link SameThreadSerializedTaskRunner}.
   */
  public SameThreadSerializedTaskRunner() {
    super();
  }

  @Override
  public boolean addTask(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException e) {
      LOG.warn("Runtime exception thrown when executing task: ", e);
    }
    return true;
  }
}
