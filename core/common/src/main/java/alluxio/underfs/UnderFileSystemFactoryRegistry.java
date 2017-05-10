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

package alluxio.underfs;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.NotThreadSafe;

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
 * Note that if you are bundling Alluxio plus your code in a shaded JAR using Maven, make sure to
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
public final class UnderFileSystemFactoryRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemFactoryRegistry.class);

  private static final List<UnderFileSystemFactory> FACTORIES = new CopyOnWriteArrayList<>();

  private static boolean sInit = false;

  // prevent instantiation
  private UnderFileSystemFactoryRegistry() {}

  static {
    // Call the actual initializer which is a synchronized method for thread safety purposes
    init();
  }

  /**
   * Returns a read-only view of the available factories.
   *
   * @return Read-only view of the available factories
   */
  public static List<UnderFileSystemFactory> available() {
    return Collections.unmodifiableList(FACTORIES);
  }

  /**
   * Finds the first Under File System factory that supports the given path.
   *
   * @param path path
   * @return factory if available, null otherwise
   */
  public static UnderFileSystemFactory find(String path) {
    Preconditions.checkArgument(path != null, "path may not be null");

    for (UnderFileSystemFactory factory : FACTORIES) {
      if (factory.supportsPath(path)) {
        LOG.debug("Selected Under File System Factory implementation {} for path {}",
            factory.getClass(), path);
        return factory;
      }
    }

    LOG.warn("No Under File System Factory implementation supports the path {}. Please check if "
        + "the under storage path is valid.", path);
    return null;
  }

  /**
   * Finds all the Under File System factories that support the given path.
   *
   * @param path path
   * @return list of factories that support the given path which may be an empty list
   */
  public static List<UnderFileSystemFactory> findAll(String path) {
    Preconditions.checkArgument(path != null, "path may not be null");

    List<UnderFileSystemFactory> eligibleFactories = new ArrayList<>();
    for (UnderFileSystemFactory factory : FACTORIES) {
      if (factory.supportsPath(path)) {
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
   * Unregisters an existing factory.
   *
   * @param factory factory to unregister
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
