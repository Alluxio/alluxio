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

package alluxio.client.file.cache;

import alluxio.client.file.cache.store.LocalPageStore;
import alluxio.client.file.cache.store.LocalPageStoreOptions;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.client.file.cache.store.RocksPageStore;
import alluxio.client.file.cache.store.RocksPageStoreOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.PageNotFoundException;
import alluxio.util.ConfigurationUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Properties;

/**
 * A simple abstraction on the storage to put, get and delete pages. The implementation of this
 * class does not need to provide thread-safety.
 */
public interface PageStore extends AutoCloseable {
  Logger LOG = LoggerFactory.getLogger(LocalPageStore.class);
  String CONF_FILE = "alluxio-client.properties";
  /**
   * Creates a new {@link PageStore}.
   *
   * @param options the options to instantiate the page store
   * @return a PageStore instance
   */
  static PageStore create(PageStoreOptions options) {
    switch (options.getType()) {
      case LOCAL:
        return new LocalPageStore(options.toOptions());
      case ROCKS:
        return new RocksPageStore(options.toOptions());
      default:
        throw new IllegalArgumentException(
            "Incompatible PageStore " + options.getType() + " specified");
    }
  }

  /**
   * Creates a new instance of {@link PageStore} based on configuration.
   * @param conf configuration
   * @return the {@link PageStore}
   */
  static PageStore create(AlluxioConfiguration conf) {
    PageStoreOptions options;
    PageStoreType storeType = conf.getEnum(
        PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.class);
    // TODO(feng): add more configurable options
    switch (storeType) {
      case LOCAL:
        options = new LocalPageStoreOptions();
        break;
      case ROCKS:
        options = new RocksPageStoreOptions();
        break;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized store type %s",
            storeType.name()));
    }
    String rootPath = conf.get(PropertyKey.USER_CLIENT_CACHE_DIR);
    options.setRootDir(rootPath);
    Path confPath = Paths.get(rootPath, CONF_FILE);
    boolean canLoad = false;
    if (Files.exists(confPath)) {
      Properties properties = ConfigurationUtils.loadPropertiesFromFile(confPath.toString());
      if (properties != null) {
        AlluxioProperties alluxioProperties = new AlluxioProperties();
        alluxioProperties.merge(properties, Source.DEFAULT);
        AlluxioConfiguration cacheConf = new InstancedConfiguration(alluxioProperties);
        // check store type
        if (cacheConf.get(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE).equals(
            conf.get(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE))
            // check page size
            && cacheConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE)
            == conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE)
            // check enough cache size
            && cacheConf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE)
            <= conf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE)
            // check alluxio version
            && cacheConf.get(PropertyKey.VERSION).equals(
            conf.get(PropertyKey.VERSION))) {
          LOG.info("Found recoverable local cache at {}", rootPath);
          canLoad = true;
        } else {
          LOG.info("Found local cache at {} with incompatible configuration.", rootPath);
        }
      }
    }
    if (!canLoad) {
      LOG.info("Clean cache directory {}", rootPath);
      File rootDir = new File(rootPath);
      try {
        if (Files.isDirectory(rootDir.toPath())) {
          FileUtils.deleteDirectory(rootDir);
        }
        FileUtils.forceMkdir(rootDir);
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format("failed to clean cache directory %s", rootDir), e);
      }
      Properties properties = new Properties();
      PropertyKey[] keys = new PropertyKey[]{
          PropertyKey.USER_CLIENT_CACHE_STORE_TYPE,
          PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE,
          PropertyKey.USER_CLIENT_CACHE_SIZE,
          PropertyKey.VERSION
      };
      for (PropertyKey key : keys) {
        properties.setProperty(key.getName(), conf.get(key));
      }
      try (FileOutputStream stream = new FileOutputStream(confPath.toString())) {
        properties.store(stream, "Alluxio local cache configuration");
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format("failed to write cache configuration to file %s", confPath), e);
      }
    }
    return create(options);
  }

  /**
   * Writes a new page from a source channel to the store.
   *
   * @param pageId page identifier
   * @param page page data
   */
  void put(PageId pageId, byte[] page) throws IOException;

  /**
   * Wraps a page from the store as a channel to read.
   *
   * @param pageId page identifier
   * @return the channel to read this page
   * @throws IOException when the store fails to read this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  default ReadableByteChannel get(PageId pageId) throws IOException,
      PageNotFoundException {
    return get(pageId, 0);
  }

  /**
   * Gets part of a page from the store to the destination channel.
   *
   * @param pageId page identifier
   * @param pageOffset offset within page
   * @return the number of bytes read
   * @throws IOException
   * @throws PageNotFoundException when the page isn't found in the store
   * @throws IllegalArgumentException when the page offset exceeds the page size
   */
  ReadableByteChannel get(PageId pageId, int pageOffset) throws IOException,
      PageNotFoundException;

  /**
   * Deletes a page from the store.
   *
   * @param pageId page identifier
   * @throws IOException when the store fails to delete this page
   * @throws PageNotFoundException when the page isn't found in the store
   */
  void delete(PageId pageId) throws IOException, PageNotFoundException;

  /**
   * @return the number of pages stored
   */
  int size();

  /**
   * Loads page store from local storage and returns all page ids.
   *
   * @return collection of ids representing all pages loaded from disk
   * @throws IOException if any error occurs
   */
  Collection<PageId> load() throws IOException;
}
