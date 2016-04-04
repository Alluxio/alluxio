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
