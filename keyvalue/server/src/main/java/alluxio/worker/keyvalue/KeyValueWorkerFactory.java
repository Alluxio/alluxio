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

package alluxio.worker.keyvalue;

import alluxio.Constants;
import alluxio.worker.Worker;
import alluxio.worker.WorkerContext;
import alluxio.worker.WorkerFactory;
import alluxio.worker.block.BlockWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link KeyValueWorker} instance.
 */
@ThreadSafe
public final class KeyValueWorkerFactory implements WorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public KeyValueWorker create(List<? extends Worker> workers) {
    if (!WorkerContext.getConf().getBoolean(Constants.KEY_VALUE_ENABLED)) {
      return null;
    }
    LOG.info("Creating {} ", KeyValueWorker.class.getName());

    for (Worker worker : workers) {
      if (worker instanceof BlockWorker) {
        LOG.info("{} is created", KeyValueWorker.class.getName());
        return new KeyValueWorker(((BlockWorker) worker));
      }
    }
    LOG.error("Fail to create {} due to missing {}", KeyValueWorker.class.getName(),
        BlockWorker.class.getName());
    return null;
  }
}
