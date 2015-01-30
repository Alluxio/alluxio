package tachyon.underfs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;

public class UnderFileSystemRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final List<UnderFileSystemFactory> factories =
      new ArrayList<UnderFileSystemFactory>();
  private static boolean init = false;

  static {
    init();
  }

  static synchronized void init() {
    if (init)
      return;

    ServiceLoader<UnderFileSystemFactory> discoverableFactories =
        ServiceLoader.load(UnderFileSystemFactory.class);
    Iterator<UnderFileSystemFactory> iter = discoverableFactories.iterator();
    while (iter.hasNext()) {
      UnderFileSystemFactory factory = iter.next();
      LOG.debug("Discovered Under File System Factory implementation {} - {}", factory.getClass(),
          factory.toString());
      factories.add(factory);
    }

    init = true;
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
    if (factory == null)
      return;

    LOG.debug("Registered Under File System Factory implementation {} - {}", factory.getClass(),
        factory.toString());
    factories.add(0, factory);
  }

  /**
   * Removes an existing factory
   * 
   * @param factory
   *          Factory to remove
   */
  public static void remove(UnderFileSystemFactory factory) {
    if (factory == null)
      return;

    LOG.debug("Unregistered Under File System Factory implementation {} - {}", factory.getClass(),
        factory.toString());
    factories.remove(factory);
  }

  /**
   * Tries to find a Under File System factory that supports the given path
   * 
   * @param path
   *          Path
   * @return Factory if available, null otherwise
   */
  public static UnderFileSystemFactory find(String path) {
    Preconditions.checkArgument(path != null, "path may not be null");

    for (UnderFileSystemFactory factory : factories) {
      if (factory.supportsPath(path)) {
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
   *          Optional configuration object, may be null
   * @return Client for the under file system
   * @throws IllegalArgumentException
   *           Thrown if there is no under file system for the given path
   */
  public static UnderFileSystem create(String path, Object conf) {
    // Try to obtain the appropriate factory
    UnderFileSystemFactory factory = find(path);
    if (factory == null)
      throw new IllegalArgumentException(String.format(
          "No known Under File System supports the given path %s", path));

    // Use the factory to create the actual client for the Under File System
    return factory.create(path, conf);
  }
}
