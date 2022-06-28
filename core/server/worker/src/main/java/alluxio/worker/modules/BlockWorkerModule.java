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

package alluxio.worker.modules;

import alluxio.ClientContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.network.TieredIdentityFactory;
import alluxio.underfs.UfsManager;
import alluxio.underfs.WorkerUfsManager;
import alluxio.wire.TieredIdentity;
import alluxio.worker.AlluxioWorkerProcess;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreType;
import alluxio.worker.block.BlockWorkerFactory;
import alluxio.worker.block.LocalBlockStore;
import alluxio.worker.block.MonoBlockStore;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.page.PagedBlockMetaStore;
import alluxio.worker.page.PagedBlockStore;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guice module for block worker.
 */
public class BlockWorkerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(TieredIdentity.class).toProvider(() ->
        TieredIdentityFactory.localIdentity(Configuration.global()));
    bind(new TypeLiteral<AtomicReference<Long>>(){}).annotatedWith(Names.named("workerId"))
        .toInstance(new AtomicReference<>(-1L));
    bind(FileSystemMasterClient.class).toProvider(() -> new FileSystemMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(Configuration.global())).build()));
    bind(UfsManager.class).to(WorkerUfsManager.class).in(Scopes.SINGLETON);
    bind(WorkerFactory.class).to(BlockWorkerFactory.class).in(Scopes.SINGLETON);
    bind(WorkerProcess.class).to(AlluxioWorkerProcess.class).in(Scopes.SINGLETON);

    switch (Configuration.global()
        .getEnum(PropertyKey.USER_BLOCK_STORE_TYPE, BlockStoreType.class)) {
      case FILE:
        bind(BlockMetadataManager.class)
            .toProvider(BlockMetadataManager::createBlockMetadataManager);
        bind(LocalBlockStore.class).to(TieredBlockStore.class).in(Scopes.SINGLETON);
        bind(BlockStore.class).to(MonoBlockStore.class).in(Scopes.SINGLETON);
        break;
      case PAGE:
        PagedBlockMetaStore pagedBlockMetaStore = new PagedBlockMetaStore(Configuration.global());
        bind(PagedBlockMetaStore.class).toInstance(pagedBlockMetaStore);
        try {
          bind(CacheManager.class).toInstance(
              CacheManager.Factory.create(Configuration.global(), pagedBlockMetaStore));
        } catch (IOException e) {
          throw new RuntimeException("Failed to create CacheManager", e);
        }
        bind(BlockStore.class).to(PagedBlockStore.class).in(Scopes.SINGLETON);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported block store type.");
    }
  }
}
