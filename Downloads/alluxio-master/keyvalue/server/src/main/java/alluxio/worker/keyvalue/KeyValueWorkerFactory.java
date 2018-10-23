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

package alluxio.worker.keyvalue;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.UfsManager;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerRegistry;
import alluxio.worker.block.BlockWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link KeyValueWorker} instance.
 */
@ThreadSafe
public final class KeyValueWorkerFactory implements WorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueWorkerFactory.class);

  /**
   * Constructs a new {@link KeyValueWorkerFactory}.
   */
  public KeyValueWorkerFactory() {}

  @Override
  public boolean isEnabled() {
    return Configuration.getBoolean(PropertyKey.KEY_VALUE_ENABLED);
  }

  @Override
  public KeyValueWorker create(WorkerRegistry registry, UfsManager ufsManager) {
    LOG.info("Creating {} ", KeyValueWorker.class.getName());
    BlockWorker blockWorker = registry.get(BlockWorker.class);
    KeyValueWorker keyValueWorker = new KeyValueWorker(blockWorker);
    registry.add(KeyValueWorker.class, keyValueWorker);
    return keyValueWorker;
  }
}
