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

import alluxio.Constants;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.metastore.BlockStore;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.MetastoreType;
import alluxio.master.metastore.caching.CachingInodeStore;
import alluxio.master.metastore.heap.HeapBlockStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksBlockStore;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.util.CommonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * This class encapsulates the different master services that are configured to run.
 */
public final class MasterUtils {
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
      callables.add(() -> {
        if (factory.isEnabled()) {
          factory.create(registry, context);
        }
        return null;
      });
    }
    try {
      CommonUtils.invokeAll(callables, 10L * Constants.MINUTE_MS);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start masters", e);
    }
  }

  /**
   * @param baseDir the base directory in which to store on-disk metadata
   * @return a block store factory of the configured type
   */
  public static BlockStore.Factory getBlockStoreFactory(String baseDir) {
    MetastoreType type =
        ServerConfiguration.getEnum(PropertyKey.MASTER_METASTORE, MetastoreType.class);
    switch (type) {
      case HEAP:
        return HeapBlockStore::new;
      case ROCKS:
        return () -> new RocksBlockStore(baseDir);
      default:
        throw new IllegalStateException("Unknown metastore type: " + type);
    }
  }

  /**
   * @param baseDir the base directory in which to store on-disk metadata
   * @return an inode store factory of the configured type
   */
  public static InodeStore.Factory getInodeStoreFactory(String baseDir) {
    MetastoreType type =
        ServerConfiguration.getEnum(PropertyKey.MASTER_METASTORE, MetastoreType.class);
    switch (type) {
      case HEAP:
        return lockManager -> new HeapInodeStore();
      case ROCKS:
        InstancedConfiguration conf = ServerConfiguration.global();
        if (conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE) == 0) {
          return lockManager -> new RocksInodeStore(baseDir);
        } else {
          return lockManager -> new CachingInodeStore(new RocksInodeStore(baseDir), lockManager);
        }
      default:
        throw new IllegalStateException("Unknown metastore type: " + type);
    }
  }
}
