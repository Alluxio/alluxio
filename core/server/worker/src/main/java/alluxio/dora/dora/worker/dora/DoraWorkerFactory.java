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

package alluxio.dora.dora.worker.dora;

import alluxio.dora.dora.conf.AlluxioConfiguration;
import alluxio.dora.dora.conf.Configuration;
import alluxio.dora.dora.conf.PropertyKey;
import alluxio.dora.dora.worker.WorkerFactory;
import alluxio.dora.dora.worker.Worker;

import com.google.inject.Inject;

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
  @Inject
  public DoraWorkerFactory(AlluxioConfiguration conf) {
    mConf = conf;
    mEnabled = mConf.getBoolean(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED);
  }

  @Override
  public boolean isEnabled() {
    return mEnabled;
  }

  @Override
  public Worker create() {
    return new PagedDoraWorker(new AtomicReference<>(-1L), mConf);
  }
}
