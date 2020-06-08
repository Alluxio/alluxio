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

package alluxio.cli;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
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
import java.util.List;
import java.util.ServiceLoader;

/**
 * The registry of validation tool implementations.
 */
public class ValidationToolRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ValidationToolRegistry.class);
  private static final String HMS_TOOL_PATTERN = "alluxio-integration-tools-hms-*.jar";

  private AlluxioConfiguration mConf;
  private ValidationToolFactory mFactory = null;

  /**
   * Creates a new instance of an {@link ValidationToolRegistry}.
   *
   * @param conf the Alluxio configuration
   */
  public ValidationToolRegistry(AlluxioConfiguration conf) {
    mConf = conf;
  }

  /**
   * Refreshes the registry by service loading classes.
   */
  public void refresh() {
    String libDir = PathUtils.concatPath(mConf.get(PropertyKey.HOME), "lib");
    LOG.info("Loading hive metastore validation tool jar from {}", libDir);
    List<File> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files
        .newDirectoryStream(Paths.get(libDir), HMS_TOOL_PATTERN)) {
      for (Path entry : stream) {
        if (entry.toFile().isFile()) {
          files.add(entry.toFile());
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to load hive metastore validation tool lib from {}. error: {}",
          libDir, e.toString());
    }

    // Load the validation tool factory from libraries
    for (File jar : files) {
      try {
        URL extensionURL = jar.toURI().toURL();
        ClassLoader extensionsClassLoader = new ExtensionsClassLoader(new URL[] {extensionURL},
            ClassLoader.getSystemClassLoader());
        for (ValidationToolFactory factory : ServiceLoader
            .load(ValidationToolFactory.class, extensionsClassLoader)) {
          if (mFactory != null) {
            LOG.warn(
                "Ignoring duplicate hms validation tool factory found in {}. Existing factory: {}",
                factory.getClass(), mFactory.getClass());
          }
          mFactory = factory;
        }
      } catch (Throwable t) {
        LOG.warn("Failed to load hms validation tool jar {}", jar, t);
      }
    }

    // Load the hive metastore validation tool from the default classloader
    for (ValidationToolFactory factory : ServiceLoader
        .load(ValidationToolFactory.class, ValidationToolRegistry.class.getClassLoader())) {
      if (mFactory != null) {
        LOG.warn("Ignoring duplicate hms validation tool factory found in {}. Existing factory: {}",
            factory.getClass(), mFactory.getClass());
      }
      mFactory = factory;
    }

    LOG.info("Registered hms validation tool factory: " + mFactory);
  }

  /**
   * Creates a new instance of {@link ValidationTool}.
   *
   * @param metastoreUri hive metastore uris
   * @param database database to run tests against
   * @param tables tables to run tests against
   * @param socketTimeout socket time of hms operations
   * @return a new validation tool instance
   */
  public ValidationTool create(String metastoreUri, String database, String tables,
      int socketTimeout) {
    if (mFactory == null) {
      throw new IllegalArgumentException("HmsValidationToolFactory does not exist.");
    }

    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Use the extension class loader of the factory.
      Thread.currentThread().setContextClassLoader(mFactory.getClass().getClassLoader());
      return mFactory.create(metastoreUri, database, tables, socketTimeout);
    } catch (Throwable e) {
      // Catching Throwable rather than Exception to catch service loading errors
      throw new IllegalStateException(
          String.format("Failed to create HmsValidationTool by factory %s", mFactory), e);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }
}
