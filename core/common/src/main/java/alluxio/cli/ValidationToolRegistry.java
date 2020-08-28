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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The registry of validation tool implementations.
 */
public class ValidationToolRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ValidationToolRegistry.class);
  private static final String VALIDATION_TOOL_PATTERN = "alluxio-integration-tools-*.jar";

  private AlluxioConfiguration mConf;
  private Map<String, ValidationToolFactory> mFactories;

  /**
   * Creates a new instance of an {@link ValidationToolRegistry}.
   *
   * @param conf the Alluxio configuration
   */
  public ValidationToolRegistry(AlluxioConfiguration conf) {
    mFactories = new HashMap<>();
    mConf = conf;
  }

  /**
   * Refreshes the registry by service loading classes.
   */
  public void refresh() {
    Map<String, ValidationToolFactory> map = new HashMap<>();

    String libDir = PathUtils.concatPath(mConf.get(PropertyKey.HOME), "lib");
    LOG.info("Loading validation tool jars from {}", libDir);
    List<File> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files
        .newDirectoryStream(Paths.get(libDir), VALIDATION_TOOL_PATTERN)) {
      for (Path entry : stream) {
        if (entry.toFile().isFile()) {
          files.add(entry.toFile());
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to load validation tool libs from {}. error: {}",
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
          ValidationToolFactory existingFactory = map.get(factory.getType());
          if (existingFactory != null) {
            LOG.warn(
                "Ignoring duplicate validation tool type '{}' found in {}. Existing factory: {}",
                factory.getType(), factory.getClass(), existingFactory.getClass());
          }
          map.put(factory.getType(), factory);
        }
      } catch (Throwable t) {
        LOG.warn("Failed to load validation tool jar {}", jar, t);
      }
    }

    // Load the validation tools from the default classloader
    for (ValidationToolFactory factory : ServiceLoader
        .load(ValidationToolFactory.class, ValidationToolRegistry.class.getClassLoader())) {
      ValidationToolFactory existingFactory = map.get(factory.getType());
      if (existingFactory != null) {
        LOG.warn("Ignoring duplicate validation tool type '{}' found in {}. Existing factory: {}",
            factory.getType(), factory.getClass(), existingFactory.getClass());
      }
      map.put(factory.getType(), factory);
    }

    mFactories = map;
    LOG.info("Registered Factories: " + String.join(",", mFactories.keySet()));
  }

  /**
   * Creates a new instance of {@link ValidationTool}.
   *
   * @param type the validation tool type
   * @param configMap the validation tool configuration tool
   * @return a new validation tool instance
   */
  public ValidationTool create(String type, Map<Object, Object> configMap) {
    Map<String, ValidationToolFactory> map = mFactories;
    ValidationToolFactory factory = map.get(type);
    if (factory == null) {
      throw new IllegalArgumentException(
          String.format("ValidationToolFactory for type '%s' does not exist.", type));
    }

    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Use the extension class loader of the factory.
      Thread.currentThread().setContextClassLoader(factory.getClass().getClassLoader());
      return factory.create(configMap);
    } catch (Throwable e) {
      // Catching Throwable rather than Exception to catch service loading errors
      throw new IllegalStateException(
          String.format("Failed to create ValidationTool by factory %s", factory), e);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }
}
