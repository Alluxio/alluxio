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

package alluxio.worker.dora;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsManager;
import alluxio.worker.DataWorker;
import alluxio.worker.Worker;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerRegistry;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for DoraWorker.
 */
public class DoraWorkerFactory implements WorkerFactory {
  private final AlluxioConfiguration mConf;
  private final boolean mEnabled;

  /**
   * Default constructor for Service Loader.
   */
  public DoraWorkerFactory() {
    this(Configuration.global());
  }

  /**
   * Constructor.
   *
   * @param conf configuration
   */
  public DoraWorkerFactory(AlluxioConfiguration conf) {
    mConf = conf;
    mEnabled = mConf.getBoolean(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED);
  }

  @Override
  public boolean isEnabled() {
    return mEnabled;
  }

  @Override
  public Worker create(WorkerRegistry registry, UfsManager ufsManager) {
    final PagedDoraWorker doraWorker = new PagedDoraWorker(new AtomicReference<>(-1L), mConf);
    registry.add(DoraWorker.class, doraWorker);
    registry.addAlias(DataWorker.class, doraWorker);
    return doraWorker;
  }
}
