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

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.metastore.Metastore;
import alluxio.master.metastore.Metastore.Type;
import alluxio.master.metastore.java.HeapMetastore;
import alluxio.master.metastore.rocks.RocksMetastore;
import alluxio.util.CommonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * This class encapsulates the different master services that are configured to run.
 */
final class MasterUtils {
  private MasterUtils() {}  // prevent instantiation

  /**
   * Creates all the masters and registers them to the master registry.
   *
   * @param registry the master registry
   * @param context master context
   */
  public static void createMasters(MasterRegistry registry, MasterContext context) {
    List<Callable<Void>> callables = new ArrayList<>();
    for (final MasterFactory factory : alluxio.master.ServiceUtils.getMasterServiceLoader()) {
      callables.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          if (factory.isEnabled()) {
            factory.create(registry, context);
          }
          return null;
        }
      });
    }
    try {
      CommonUtils.invokeAll(callables, 10 * Constants.SECOND_MS);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start masters", e);
    }
  }

  /**
   * @return a metastore of the configured type
   */
  public static Metastore getMetaStore() {
    Type type = Configuration.getEnum(PropertyKey.MASTER_METASTORE, Type.class);
    switch (type) {
      case HEAP:
        return new HeapMetastore();
      case ROCKS:
        return new RocksMetastore();
      default:
        throw new IllegalStateException("Unknown metastore type: " + type);
    }
  }
}
