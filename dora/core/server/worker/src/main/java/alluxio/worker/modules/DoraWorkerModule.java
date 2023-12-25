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
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.membership.MembershipManager;
import alluxio.membership.MembershipType;
import alluxio.metrics.MultiDimensionalMetricsSystem;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerIdentity;
import alluxio.worker.Worker;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.dora.DoraMetaManager;
import alluxio.worker.dora.DoraUfsManager;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedDoraWorker;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.http.HttpServer;
import alluxio.worker.http.HttpServerInitializer;
import alluxio.worker.http.PagedService;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guice module for block worker.
 */
public class DoraWorkerModule extends AbstractModule {
  @Override
  protected void configure() {
    if (Configuration.getEnum(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.class)
        == MembershipType.MASTER) {
      bind(new TypeLiteral<AtomicReference<WorkerIdentity>>() {
      })
          .annotatedWith(Names.named("workerId"))
          .toInstance(new AtomicReference<>(null));
    } else { // for etcd-based and static membership managers
      bind(WorkerIdentity.class)
          .toProvider(WorkerIdentityProvider.class)
          .in(Scopes.SINGLETON);
      Provider<WorkerIdentity> workerIdentityProvider = getProvider(WorkerIdentity.class);
      bind(new TypeLiteral<AtomicReference<WorkerIdentity>>() {
      })
          .annotatedWith(Names.named("workerId"))
          .toProvider(() -> new AtomicReference<>(workerIdentityProvider.get()))
          .in(Scopes.SINGLETON);
    }

    bind(FileSystemMasterClient.class).toProvider(() -> new FileSystemMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(Configuration.global())).build()));
    bind(BlockMasterClientPool.class)
        .toProvider(BlockMasterClientPool::new)
        .in(Scopes.SINGLETON);
    bind(UfsManager.class).to(DoraUfsManager.class).in(Scopes.SINGLETON);
    bind(DoraMetaManager.class).in(Scopes.SINGLETON);
    bind(AlluxioConfiguration.class).toProvider(() -> Configuration.global());

    // Create FileSystemContext shared across all worker components
    FileSystemContext fileSystemContext = FileSystemContext.create();
    bind(FileSystemContext.class).toInstance(fileSystemContext);

    // Note that dora can only use Paged Store
    try {
      CacheManagerOptions cacheManagerOptions =
          CacheManagerOptions.createForWorker(Configuration.global());

      PageMetaStore pageMetaStore = PageMetaStore.create(
          CacheManagerOptions.createForWorker(Configuration.global()));
      bind(PageMetaStore.class).toInstance(pageMetaStore);
      MultiDimensionalMetricsSystem.setCacheStorageSupplier(pageMetaStore::bytes);
      bind(CacheManager.class).toProvider(() ->
      {
        try {
          return CacheManager.Factory.create(Configuration.global(),
              cacheManagerOptions, pageMetaStore);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).in(Scopes.SINGLETON);
      bind(MembershipManager.class)
          .toProvider(() -> MembershipManager.Factory.create(Configuration.global()))
          .in(Scopes.SINGLETON);

      long pageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
      bind(new TypeLiteral<Long>() {
      }).annotatedWith(Names.named("pageSize")).toInstance(pageSize);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create CacheManager", e);
    }

    // HTTP Server
    bind(FileSystemContext.FileSystemContextFactory.class).in(Scopes.SINGLETON);
    bind(PagedService.class).in(Scopes.SINGLETON);
    bind(HttpServerInitializer.class).in(Scopes.SINGLETON);
    bind(HttpServer.class).in(Scopes.SINGLETON);

    // the following objects are required when using dora
    bind(Worker.class).to(DoraWorker.class).in(Scopes.SINGLETON);
    bind(DoraWorker.class).to(PagedDoraWorker.class).in(Scopes.SINGLETON);
  }
}
