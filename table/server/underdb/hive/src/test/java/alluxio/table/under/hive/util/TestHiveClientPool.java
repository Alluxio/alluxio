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

package alluxio.table.under.hive.util;

import alluxio.exception.status.UnimplementedException;
import alluxio.resource.CloseableResource;
import alluxio.util.ThreadFactoryUtils;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * A pool that does nothing, used for testing.
 */
public class TestHiveClientPool extends AbstractHiveClientPool {
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(1, ThreadFactoryUtils.build("TestHiveClientPool-GC-%d", true));
  /**
   * Creates a new instance with default options from {@link Options#defaultOptions()}.
   */
  public TestHiveClientPool() {
    super(Options.defaultOptions().setGcExecutor(GC_EXECUTOR));
  }

  /**
   * Creates a new instance.
   * @param options options of the resource pool
   */
  public TestHiveClientPool(Options options) {
    super(options);
  }

  @Override
  protected boolean shouldGc(ResourceInternal<IMetaStoreClient> resourceInternal) {
    return false;
  }

  @Override
  protected boolean isHealthy(IMetaStoreClient resource) {
    return true;
  }

  @Override
  protected void closeResource(IMetaStoreClient resource) throws IOException {}

  @Override
  protected IMetaStoreClient createNewResource() throws IOException {
    throw new UnsupportedOperationException("createNewResource");
  }

  @Override
  public CloseableResource<IMetaStoreClient> acquireClientResource() throws IOException {
    throw new UnsupportedOperationException("acquireClientResource");
  }
}
