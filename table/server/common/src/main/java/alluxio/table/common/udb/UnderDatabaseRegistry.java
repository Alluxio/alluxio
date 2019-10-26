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

package alluxio.table.common.udb;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.extensions.ExtensionsClassLoader;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The registry of under database implementations.
 */
public class UnderDatabaseRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(UnderDatabaseRegistry.class);
  private static final String UDB_EXTENSION_PATTERN = "alluxio-table-server-underdb-*.jar";

  private volatile Map<String, UnderDatabaseFactory> mFactories;

  /**
   * Creates an instance.
   */
  public UnderDatabaseRegistry() {
    mFactories = new HashMap<>();
  }

  /**
   * Refreshes the registry by service loading classes.
   */
  public void refresh() {
    Map<String, UnderDatabaseFactory> map = new HashMap<>();

    String libDir =
        PathUtils.concatPath(ServerConfiguration.global().get(PropertyKey.HOME), "lib");
    LOG.info("Loading udb jars from {}", libDir);
    List<File> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files
        .newDirectoryStream(Paths.get(libDir), UDB_EXTENSION_PATTERN)) {
      for (Path entry : stream) {
        if (entry.toFile().isFile()) {
          files.add(entry.toFile());
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to load udb libs from {}. error: {}", libDir, e.toString());
    }

    // Load the UDBs from libraries
    for (File jar : files) {
      try {
        URL extensionURL = jar.toURI().toURL();
        ClassLoader extensionsClassLoader = new ExtensionsClassLoader(new URL[] {extensionURL},
            ClassLoader.getSystemClassLoader());

        for (UnderDatabaseFactory factory : ServiceLoader
            .load(UnderDatabaseFactory.class, extensionsClassLoader)) {
          UnderDatabaseFactory existingFactory = map.get(factory.getType());
          if (existingFactory != null) {
            LOG.warn(
                "Ignoring duplicate under database type '{}' found in {}. Existing factory: {}",
                factory.getType(), factory.getClass(), existingFactory.getClass());
          }
          map.put(factory.getType(), factory);
        }
      } catch (Throwable t) {
        LOG.warn("Failed to load udb jar {}", jar, t);
      }
    }

    // Load the UDBs from the default classloader
    for (UnderDatabaseFactory factory : ServiceLoader
        .load(UnderDatabaseFactory.class, UnderDatabaseRegistry.class.getClassLoader())) {
      UnderDatabaseFactory existingFactory = map.get(factory.getType());
      if (existingFactory != null) {
        LOG.warn("Ignoring duplicate under database type '{}' found in {}. Existing factory: {}",
            factory.getType(), factory.getClass(), existingFactory.getClass());
      }
      map.put(factory.getType(), factory);
    }

    mFactories = map;
    LOG.info("Registered UDBs: " + String.join(",", mFactories.keySet()));
  }

  /**
   * Creates a new instance of an {@link UnderDatabase}.
   *
   * @param udbContext the db context
   * @param type the udb type
   * @param configuration the udb configuration
   * @return a new udb instance
   */
  public UnderDatabase create(UdbContext udbContext, String type, UdbConfiguration configuration) {
    Map<String, UnderDatabaseFactory> map = mFactories;
    UnderDatabaseFactory factory = map.get(type);
    if (factory == null) {
      throw new IllegalArgumentException(
          String.format("UdbFactory for type '%s' does not exist.", type));
    }

    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Use the extension class loader of the factory.
      Thread.currentThread().setContextClassLoader(factory.getClass().getClassLoader());
      return factory.create(udbContext, configuration);
    } catch (Throwable e) {
      // Catching Throwable rather than Exception to catch service loading errors
      throw new IllegalStateException(
          String.format("Failed to create UnderDb by factory %s", factory), e);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }
}
