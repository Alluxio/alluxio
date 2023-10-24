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

import alluxio.client.file.MetadataCache;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.dora.DefaultDoraCacheClientFactory;
import alluxio.client.file.dora.DoraCacheClientFactory;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.InternalRuntimeException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.OptionalBinder;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * Guice module for dora client filesystem.
 */
public class DoraFileSystemModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(DoraFileSystemModule.class);

  @Override
  protected void configure() {
    bind(DoraCacheClientFactory.class).to(DefaultDoraCacheClientFactory.class);
    bind(FileSystemOptions.class)
        .toInstance(FileSystemOptions.Builder.fromConf(Configuration.global()).build());
    OptionalBinder.newOptionalBinder(binder(), CacheManager.class);
    OptionalBinder.newOptionalBinder(binder(), MetadataCache.class);
  }

  /**
   * @return the local cache manager
   */
  @Provides
  @Named("LocalCacheManager")
  public Optional<CacheManager> provideCacheManager() {
    try {
      CacheManager cacheManager = CacheManager.Factory.get(Configuration.global());
      return Optional.of(cacheManager);
    } catch (IOException e) {
      String err = "can't construct the cache manager.";
      LOG.error(err);
      throw new InternalRuntimeException(err, e);
    }
  }

  /**
   * @return the local cache manager
   */
  @Provides
  @Named("MetadataCache")
  public Optional<MetadataCache> provideMetadataCache() {
    int maxSize = Configuration.getInt(PropertyKey.USER_METADATA_CACHE_MAX_SIZE);
    return Configuration.isSet(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME)
        ? Optional.of(new MetadataCache(maxSize,
        Configuration.getMs(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME)))
        : Optional.of(new MetadataCache(maxSize));
  }
}
