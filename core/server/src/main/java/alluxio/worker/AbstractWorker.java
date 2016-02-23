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

package alluxio.worker;

import alluxio.Constants;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;
/**
 * This is the base class for all workers, and contains common functionality.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public abstract class AbstractWorker implements Worker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The executor service for the master sync. */
  private final ExecutorService mExecutorService;

  /**
   * @param executorService executor service to use internally
   */
  protected AbstractWorker(ExecutorService executorService)  {
    mExecutorService = Preconditions.checkNotNull(executorService);
  }

  /**
   * @return the executor service
   */
  protected ExecutorService getExecutorService() {
    return mExecutorService;
  }
}
