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
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.network.TieredIdentityFactory;
import alluxio.underfs.UfsManager;
import alluxio.wire.TieredIdentity;
import alluxio.worker.Worker;
import alluxio.worker.dora.DoraUfsManager;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedDoraWorker;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.http.HttpServer;
import alluxio.worker.http.HttpServerInitializer;
import alluxio.worker.http.PagedService;

import com.google.inject.AbstractModule;
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
    bind(TieredIdentity.class).toProvider(() ->
        TieredIdentityFactory.localIdentity(Configuration.global()));
    bind(new TypeLiteral<AtomicReference<Long>>() {
    }).annotatedWith(Names.named("workerId"))
        .toInstance(new AtomicReference<>(-1L));
    bind(FileSystemMasterClient.class).toProvider(() -> new FileSystemMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(Configuration.global())).build()));
    bind(UfsManager.class).to(DoraUfsManager.class).in(Scopes.SINGLETON);
    bind(AlluxioConfiguration.class).toProvider(() -> Configuration.global());

    // Note that dora can only use Paged Store
    try {
      CacheManagerOptions cacheManagerOptions =
          CacheManagerOptions.createForWorker(Configuration.global());

      PageMetaStore pageMetaStore = PageMetaStore.create(
          CacheManagerOptions.createForWorker(Configuration.global()));
      bind(PageMetaStore.class).toInstance(pageMetaStore);
      bind(CacheManager.class).toProvider(() ->
      {
        try {
          return CacheManager.Factory.create(Configuration.global(),
              cacheManagerOptions, pageMetaStore);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).in(Scopes.SINGLETON);

      long pageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
      bind(new TypeLiteral<Long>() {
      }).annotatedWith(Names.named("pageSize")).toInstance(pageSize);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create CacheManager", e);
    }

    // HTTP Server
    bind(PagedService.class).in(Scopes.SINGLETON);
    bind(HttpServerInitializer.class).in(Scopes.SINGLETON);
    bind(HttpServer.class).in(Scopes.SINGLETON);

    // the following objects are required when using dora
    bind(Worker.class).to(DoraWorker.class).in(Scopes.SINGLETON);
    bind(DoraWorker.class).to(PagedDoraWorker.class).in(Scopes.SINGLETON);
  }
}
