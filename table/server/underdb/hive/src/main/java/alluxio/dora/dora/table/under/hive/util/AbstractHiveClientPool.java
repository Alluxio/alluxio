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

package alluxio.dora.dora.table.under.hive.util;

import alluxio.dora.dora.resource.CloseableResource;
import alluxio.dora.dora.resource.DynamicResourcePool;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.io.IOException;

/**
 * A pool for hive clients.
 */
public abstract class AbstractHiveClientPool extends DynamicResourcePool<IMetaStoreClient> {
  protected AbstractHiveClientPool(Options options) {
    super(options);
  }

  /**
   * @return a closeable resource for the hive client
   */
  public abstract CloseableResource<IMetaStoreClient> acquireClientResource() throws IOException;
}
