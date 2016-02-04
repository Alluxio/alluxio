/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.underfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.conf.TachyonConf;

/**
 * <p>
 * Central registry of available {@link UnderFileSystemFactory} instances that uses the
 * {@link ServiceLoader} mechanism to automatically discover available factories and provides a
 * central place for obtaining actual {@link UnderFileSystem} instances.
 * </p>
 * <h3>Registering New Factories</h3>
 * <p>
 * New factories can be registered either using the {@linkplain ServiceLoader} based automatic
 * discovery mechanism or manually using the static {@link #register(UnderFileSystemFactory)}
 * method. The down-side of the automatic discovery mechanism is that the discovery order is not
 * controllable. As a result if your implementation is designed as a replacement for one of the
 * standard implementations, depending on the order in which the JVM discovers the services your own
 * implementation may not take priority. You can enable {@code DEBUG} level logging for this class
 * to see the order in which factories are discovered and which is selected when obtaining a
 * {@link UnderFileSystem} instance for a path. If this shows that your implementation is not
 * getting discovered or used, you may wish to use the manual registration approach.
 * </p>
 * <h4>Automatic Discovery</h4>
 * <p>
 * To use the {@linkplain ServiceLoader} based mechanism you need to have a file named
 * {@code alluxio.underfs.UnderFileSystemFactory} placed in the {@code META-INF\services} directory
 * of your project. This file should contain the full name of your factory types (one per line),
 * your factory types must have a public unparameterised constructor available (see
 * {@link ServiceLoader} for more detail on this). You can enable {@code DEBUG} level logging to see
 * factories as they are discovered if you wish to check that your implementation gets discovered.
 * </p>
 * <p>
 * Note that if you are bundling Tachyon plus your code in a shaded JAR using Maven, make sure to
 * use the {@code ServicesResourceTransformer} as otherwise your services file will override the
 * core provided services file and leave the standard factories and under file system
 * implementations unavailable.
 * </p>
 * <h4>Manual Registration</h4>
 * <p>
 * To manually register a factory, simply pass an instance of your factory to the
 * {@link #register(UnderFileSystemFactory)} method. This can be useful when your factory cannot be
 * instantiated without arguments or in cases where automatic discovery does not give your factory
 * priority. Factories registered this way will be registered at the start of the factories list so
 * will have the first opportunity to indicate whether they support a requested path.
 * </p>
 */
@NotThreadSafe
public final class UnderFileSystemRegistry {

  private static final List<UnderFileSystemFactory> FACTORIES =
      new CopyOnWriteArrayList<UnderFileSystemFactory>();
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static boolean sInit = false;

  static {
    // Call the actual initializer which is a synchronized method for thread safety purposes
    init();
  }

  /**
   * Returns a read-only view of the available factories
   *
   * @return Read-only view of the available factories
   */
  public static List<UnderFileSystemFactory> available() {
    return Collections.unmodifiableList(FACTORIES);
  }

  /**
   * Creates a client for operations involved with the under file system. An
   * {@link IllegalArgumentException} is thrown if there is no under file system for the given path
   * or if no under file system could successfully be created.
   *
   * @param path Path
   * @param tachyonConf Tachyon Configuration
   * @param ufsConf Optional configuration object for the UFS, may be null
   * @return Client for the under file system
   */
  public static UnderFileSystem create(String path, TachyonConf tachyonConf, Object ufsConf) {
    // Try to obtain the appropriate factory
    List<UnderFileSystemFactory> factories = findAll(path, tachyonConf);
    if (factories.isEmpty()) {
      throw new IllegalArgumentException("No Under File System Factory found for: " + path);
    }

    List<Throwable> errors = new ArrayList<Throwable>();
    for (UnderFileSystemFactory factory : factories) {
      try {
        // Use the factory to create the actual client for the Under File System
        return factory.create(path, tachyonConf, ufsConf);
      } catch (Exception e) {
        errors.add(e);
      }
    }

    // If we reach here no factories were able to successfully create for this path likely due to
    // missing configuration since if we reached here at least some factories claimed to support the
    // path
    // Need to collate the errors
    StringBuilder errorStr = new StringBuilder();
    errorStr.append("All eligible Under File Systems were unable to create an instance for the "
        + "given path: ").append(path).append('\n');
    for (Throwable e : errors) {
      errorStr.append(e).append('\n');
    }
    throw new IllegalArgumentException(errorStr.toString());
  }

  /**
   * Finds the first Under File System factory that supports the given path
   *
   * @param path Path
   * @param tachyonConf Tachyon configuration
   * @return Factory if available, null otherwise
   */
  public static UnderFileSystemFactory find(String path, TachyonConf tachyonConf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    for (UnderFileSystemFactory factory : FACTORIES) {
      if (factory.supportsPath(path, tachyonConf)) {
        LOG.debug("Selected Under File System Factory implementation {} for path {}",
            factory.getClass(), path);
        return factory;
      }
    }

    LOG.warn("No Under File System Factory implementation supports the path {}", path);
    return null;
  }

  /**
   * Finds all the Under File System factories that support the given path
   *
   * @param path Path
   * @param tachyonConf Tachyon Configuration
   * @return List of factories that support the given path which may be an empty list
   */
  public static List<UnderFileSystemFactory> findAll(String path, TachyonConf tachyonConf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    List<UnderFileSystemFactory> eligibleFactories = new ArrayList<UnderFileSystemFactory>();
    for (UnderFileSystemFactory factory : FACTORIES) {
      if (factory.supportsPath(path, tachyonConf)) {
        LOG.debug("Under File System Factory implementation {} is eligible for path {}",
            factory.getClass(), path);
        eligibleFactories.add(factory);
      }
    }

    if (eligibleFactories.isEmpty()) {
      LOG.warn("No Under File System Factory implementation supports the path {}", path);
    }
    return eligibleFactories;
  }

  private static synchronized void init() {
    if (sInit) {
      return;
    }

    // Discover and register the available factories
    ServiceLoader<UnderFileSystemFactory> discoveredFactories =
        ServiceLoader.load(UnderFileSystemFactory.class,
            UnderFileSystemFactory.class.getClassLoader());
    for (UnderFileSystemFactory factory : discoveredFactories) {
      LOG.debug("Discovered Under File System Factory implementation {} - {}", factory.getClass(),
          factory.toString());
      FACTORIES.add(factory);
    }

    sInit = true;
  }

  /**
   * Registers a new factory
   * <p>
   * Factories are registered at the start of the factories list so they can override the existing
   * automatically discovered factories. Generally if you use the {@link ServiceLoader} mechanism
   * properly it should be unnecessary to call this, however since ServiceLoader discovery order
   * may be susceptible to class loader behavioural differences there may be rare cases when you
   * need to manually register the desired factory.
   * </p>
   *
   * @param factory Factory to register
   */
  public static void register(UnderFileSystemFactory factory) {
    if (factory == null) {
      return;
    }

    LOG.debug("Registered Under File System Factory implementation {} - {}", factory.getClass(),
        factory.toString());

    // Insert at start of list so it will take precedence over automatically discovered and
    // previously registered factories
    FACTORIES.add(0, factory);
  }

  /**
   * Resets the registry to its default state
   * <p>
   * This clears the registry as it stands and rediscovers the available factories.
   * </p>
   */
  public static synchronized void reset() {
    if (sInit) {
      // Reset state
      sInit = false;
      FACTORIES.clear();
    }

    // Reinitialise
    init();
  }

  /**
   * Unregisters an existing factory
   *
   * @param factory Factory to unregister
   */
  public static void unregister(UnderFileSystemFactory factory) {
    if (factory == null) {
      return;
    }

    LOG.debug("Unregistered Under File System Factory implementation {} - {}", factory.getClass(),
        factory.toString());
    FACTORIES.remove(factory);
  }
}
