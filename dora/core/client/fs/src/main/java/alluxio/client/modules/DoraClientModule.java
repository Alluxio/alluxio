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

package alluxio.client.modules;

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.dora.DefalutDoraCacheClientFactory;
import alluxio.client.file.dora.DoraCacheClientFactory;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.conf.Configuration;
import alluxio.namespace.MountTableManager;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.OptionalBinder;

/**
 * Guice module for dora client.
 */
public class DoraClientModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(DoraCacheClientFactory.class).to(DefalutDoraCacheClientFactory.class);
    bind(FileSystemOptions.class)
        .toInstance(FileSystemOptions.Builder.fromConf(Configuration.global()).build());
    OptionalBinder.newOptionalBinder(binder(), MountTableManager.class);
    OptionalBinder.newOptionalBinder(binder(), CacheManager.class);
  }
}
