/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
