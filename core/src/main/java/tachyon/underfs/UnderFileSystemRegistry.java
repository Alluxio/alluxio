package tachyon.underfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * <p>
 * Central registry of available {@link UnderFileSystemFactory} instances that uses the
 * {@link ServiceLoader} mechanism to automatically discover available factories and provides a
 * central place for obtaining actual {@link UnderFileSystem} instances.
 * </p>
 * <h3>Registering New Factories</h3>
 * <p>
 * New factories can be registered either using the {@linkplain ServiceLoader} based automatic
 * discovery mechanism or manually using the static {@link #add(UnderFileSystemFactory)} method. The
 * down-side of the automatic discovery mechanism is that the discovery order is not controllable so
 * if your implementation is designed as a replacement for one of the standard implementations
 * depending on the order in which the JVM discovers the services your own implementation may not
 * take priority. You can enable {@code DEBUG} level logging for this class to see the order in
 * which factories are discovered and which is selected when obtaining a {@link UnderFileSystem}
 * instance for a path. If this shows that your implementation is not getting discovered or used
 * then you may wish to use the manual registration approach.
 * <p/>
 * <h4>Automatic Discovery</h4>
 * <p>
 * To use the {@linkplain ServiceLoader} based mechanism you need to have a file named
 * {@code tachyon.underfs.UnderFileSystemFactory} placed in the {@code META-INF\services} directory
 * of your project. This file should contain the full name of your factory types (one per line),
 * your factory types must have a public unparameterised constructor available (see
 * {@link ServiceLoader} for more detail on this). You can enable {@code DEBUG} level logging to see
 * factories as they are discovered if you wish to check that your implementation gets discovered.
 * </p>
 * <p>
 * Note that if you are bundling Tachyon plus your code in a shaded JAR using Maven make sure to use
 * the {@code ServicesResourceTransformer} as otherwise your services file will override the core
 * provided services file and leave the standard factories and under file system implementations
 * unavailable.
 * </p>
 * <h4>Manual Registration</h4>
 * <p>
 * To manually register a factory simply pass an instance of your factory to the
 * {@link #add(UnderFileSystemFactory)} method. This can be useful when your factory cannot be
 * instantiated without arguments or in cases where automatic discovery does not give your factory
 * priority. Factories registered this way will be registered at the start of the factories list so
 * will have the first opportunity to indicate whether they support a requested path.
 * </p>
 */
public class UnderFileSystemRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final List<UnderFileSystemFactory> FACTORIES =
      new CopyOnWriteArrayList<UnderFileSystemFactory>();
  private static boolean sInit = false;

  static {
    // Call the actual initializer which is a synchronized method for thread safety purposes
    init();
  }

  static synchronized void init() {
    if (sInit) {
      return;
    }

    // Discover and register the available factories
    ServiceLoader<UnderFileSystemFactory> discoverableFactories =
        ServiceLoader.load(UnderFileSystemFactory.class);
    Iterator<UnderFileSystemFactory> iter = discoverableFactories.iterator();
    while (iter.hasNext()) {
      UnderFileSystemFactory factory = iter.next();
      LOG.debug("Discovered Under File System Factory implementation {} - {}", factory.getClass(),
          factory.toString());
      FACTORIES.add(factory);
    }

    sInit = true;
  }

  /**
   * Resets the registry to its default state
   * <p>
   * This clears the registry as it stands and rediscovers the available factories.
   * </p>
   */
  public static synchronized void reset() {
    // Can't reset if we're not initialized
    if (!sInit) {
      return;
    }

    // Reset state
    sInit = false;
    FACTORIES.clear();

    // Reinitialise
    init();
  }

  /**
   * Adds a new factory
   * <p>
   * Factories are added to the start of the factories list so they can override the existing
   * automatically discovered factories. Generally if you use the ServiceLoader mechanism properly
   * it should be unnecessary to call this, however since ServiceLoader discovery order may be
   * susceptible to class loader behavioural differences there may be rare cases when you need to
   * manually register the desired factory.
   * <p>
   * 
   * @param factory
   *          Factory to add
   */
  public static void add(UnderFileSystemFactory factory) {
    if (factory == null) {
      return;
    }

    LOG.debug("Registered Under File System Factory implementation {} - {}", factory.getClass(),
        factory.toString());
    FACTORIES.add(0, factory);
  }

  /**
   * Removes an existing factory
   * 
   * @param factory
   *          Factory to remove
   */
  public static void remove(UnderFileSystemFactory factory) {
    if (factory == null) {
      return;
    }

    LOG.debug("Unregistered Under File System Factory implementation {} - {}", factory.getClass(),
        factory.toString());
    FACTORIES.remove(factory);
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
   * Tries to find a Under File System factory that supports the given path
   * 
   * @param path
   *          Path
   * @param Tachyon
   *          configuration
   * @return Factory if available, null otherwise
   */
  public static UnderFileSystemFactory find(String path, TachyonConf conf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    for (UnderFileSystemFactory factory : FACTORIES) {
      if (factory.supportsPath(path, conf)) {
        LOG.debug("Selected Under File System Factory implementation {} for path {}",
            factory.getClass(), path);
        return factory;
      }
    }

    LOG.warn("No Under File System Factory implementation supports the path {}", path);
    return null;
  }

  /**
   * Creates a client that can talk to the under file system
   * 
   * @param path
   *          Path
   * @param conf
   *          Optional configuration object for the UFS, may be null
   * @param tachyonConf
   *          Tachyon Configuration
   * @return Client for the under file system
   * @throws IllegalArgumentException
   *           Thrown if there is no under file system for the given path
   */
  public static UnderFileSystem create(String path, TachyonConf tachyonConf, Object conf) {
    // Try to obtain the appropriate factory
    UnderFileSystemFactory factory = find(path, tachyonConf);
    if (factory == null) {
      throw new IllegalArgumentException(String.format(
          "No known Under File System supports the given path %s", path));
    }

    // Use the factory to create the actual client for the Under File System
    return factory.create(path, tachyonConf, conf);
  }
}
