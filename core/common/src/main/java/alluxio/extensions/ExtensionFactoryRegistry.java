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

package alluxio.extensions;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ExtensionUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * An extension registry that uses the {@link ServiceLoader} mechanism to automatically discover
 * available factories and provides a central place for obtaining actual extension instances.
 * </p>
 * <h3>Registering New Factories</h3>
 * <p>
 * New factories can be registered either using the {@linkplain ServiceLoader} based automatic
 * discovery mechanism or manually using the static {@link #register(T)}
 * method. The down-side of the automatic discovery mechanism is that the discovery order is not
 * controllable. As a result if your implementation is designed as a replacement for one of the
 * standard implementations, depending on the order in which the JVM discovers the services your own
 * implementation may not take priority. You can enable {@code DEBUG} level logging for this class
 * to see the order in which factories are discovered and which is selected when obtaining a
 * {@link T} instance for a path. If this shows that your implementation is not
 * getting discovered or used, you may wish to use the manual registration approach.
 * </p>
 * <h4>Automatic Discovery</h4>
 * <p>
 * To use the {@linkplain ServiceLoader} based mechanism you need to have a file with the same name
 * as the factory class {@code T} placed in the {@code META-INF\services} directory
 * of your project. This file should contain the full name of your factory types (one per line),
 * your factory types must have a public unparameterised constructor available (see
 * {@link ServiceLoader} for more detail on this). You can enable {@code DEBUG} level logging to see
 * factories as they are discovered if you wish to check that your implementation gets discovered.
 * </p>
 * <h4>Manual Registration</h4>
 * <p>
 * To manually register a factory, simply pass an instance of your factory to the
 * {@link #register(T)} method. This can be useful when your factory cannot be
 * instantiated without arguments or in cases where automatic discovery does not give your factory
 * priority. Factories registered this way will be registered at the start of the factories list so
 * will have the first opportunity to indicate whether they support a requested path.
 * </p>
 * @param <T> The type of extension factory
 * @param <S> the type of configuration to be used when creating the extension
 */
@NotThreadSafe
public class ExtensionFactoryRegistry<T extends ExtensionFactory<?, S>,
    S extends AlluxioConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ExtensionFactoryRegistry.class);

  /**
   * The base list of factories, which does not include any lib or extension factories. The only
   * factories in the base list will be built-in factories, and any additional factories
   * registered by tests. All other factories will be discovered and service loaded when extension
   * creation occurs.
   */
  private final List<T> mFactories = new CopyOnWriteArrayList<>();
  private final String mExtensionPattern;
  private final Class<T> mFactoryClass;
  private boolean mInit = false;

  /**
   * Constructs a registry for loading extension of a particular type.
   * @param factoryClass the type of the extension factory
   * @param extensionPattern the pattern used to select libraries to be loaded
   */
  public ExtensionFactoryRegistry(Class<T> factoryClass, String extensionPattern) {
    mFactoryClass = Preconditions.checkNotNull(factoryClass, "factoryClass");
    mExtensionPattern = extensionPattern;
    init();
  }

  private synchronized void init() {
    // Discover and register the available base factories
    ServiceLoader<T> discoveredFactories =
        ServiceLoader.load(mFactoryClass, mFactoryClass.getClassLoader());
    for (T factory : discoveredFactories) {
      LOG.debug("Discovered base extension factory implementation {} - {}", factory.getClass(),
          factory);
      register(factory);
    }
  }

  /**
   * Returns a read-only view of the available base factories.
   *
   * @return Read-only view of the available base factories
   */
  public List<T> getAvailable() {
    return Collections.unmodifiableList(mFactories);
  }

  /**
   * Finds all the factories that support the given path.
   *
   * @param path path
   * @param conf configuration of the extension
   * @return list of factories that support the given path which may be an empty list
   */
  public List<T> findAll(String path, S conf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    List<T> factories = new ArrayList<>(mFactories);
    String libDir = PathUtils.concatPath(conf.get(PropertyKey.HOME), "lib");
    String extensionDir = conf.get(PropertyKey.EXTENSIONS_DIR);
    scanLibs(factories, libDir);
    scanExtensions(factories, extensionDir);

    List<T> eligibleFactories = new ArrayList<>();
    for (T factory : factories) {
      if (factory.supportsPath(path, conf)) {
        LOG.debug("Factory implementation {} is eligible for path {}", factory, path);
        eligibleFactories.add(factory);
      }
    }

    if (eligibleFactories.isEmpty()) {
      LOG.warn("No factory implementation supports the path {}", path);
    }
    return eligibleFactories;
  }

  /**
   * Finds all factory from the extensions directory.
   *
   * @param factories list of factories to add to
   */
  private void scanExtensions(List<T> factories, String extensionsDir) {
    LOG.info("Loading extension jars from {}", extensionsDir);
    scan(Arrays.asList(ExtensionUtils.listExtensions(extensionsDir)), factories);
  }

  /**
   * Finds all factory from the lib directory.
   *
   * @param factories list of factories to add to
   */
  private void scanLibs(List<T> factories, String libDir) {
    LOG.info("Loading core jars from {}", libDir);
    List<File> files = new ArrayList<>();
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(Paths.get(libDir), mExtensionPattern)) {
      for (Path entry : stream) {
        if (entry.toFile().isFile()) {
          files.add(entry.toFile());
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to load libs: {}", e.toString());
    }
    scan(files, factories);
  }

  /**
   * Class-loads jar files that have not been loaded.
   *
   * @param files jar files to class-load
   * @param factories list of factories to add to
   */
  private void scan(List<File> files, List<T> factories) {
    for (File jar : files) {
      try {
        URL extensionURL = jar.toURI().toURL();
        String jarPath = extensionURL.toString();

        ClassLoader extensionsClassLoader = new ExtensionsClassLoader(new URL[] {extensionURL},
            ClassLoader.getSystemClassLoader());
        ServiceLoader<T> extensionServiceLoader =
            ServiceLoader.load(mFactoryClass, extensionsClassLoader);
        for (T factory : extensionServiceLoader) {
          LOG.debug("Discovered a factory implementation {} - {} in jar {}", factory.getClass(),
              factory, jarPath);
          register(factory, factories);
        }
      } catch (Throwable t) {
        LOG.warn("Failed to load jar {}: {}", jar, t.toString());
      }
    }
  }

  /**
   * Registers a new factory.
   * <p>
   * Factories are registered at the start of the factories list so they can override the existing
   * automatically discovered factories. Generally if you use the {@link ServiceLoader} mechanism
   * properly it should be unnecessary to call this, however since ServiceLoader discovery order
   * may be susceptible to class loader behavioral differences there may be rare cases when you
   * need to manually register the desired factory.
   * </p>
   *
   * @param factory factory to register
   */
  public void register(T factory) {
    register(factory, mFactories);
  }

  private void register(T factory, List<T> factories) {
    if (factory == null) {
      return;
    }

    LOG.debug("Registered factory implementation {} - {}", factory.getClass(), factory);

    // Insert at start of list so it will take precedence over automatically discovered and
    // previously registered factories
    factories.add(0, factory);
  }

  /**
   * Resets the registry to its default state
   * <p>
   * This clears the registry as it stands and rediscovers the available factories.
   * </p>
   */
  public synchronized void reset() {
    if (mInit) {
      // Reset state
      mInit = false;
      mFactories.clear();
    }

    // Reinitialise
    init();
  }

  /**
   * Unregisters an existing factory.
   *
   * @param factory factory to unregister
   */
  public void unregister(T factory) {
    unregister(factory, mFactories);
  }

  private void unregister(T factory, List<T> factories) {
    if (factory == null) {
      return;
    }

    LOG.debug("Unregistered factory implementation {} - {}", factory.getClass(), factory);
    factories.remove(factory);
  }
}
