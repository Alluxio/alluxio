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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.extensions.ExtensionFactoryRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * Central registry of available {@link UnderFileSystemFactory} instances that uses the
 * {@link ServiceLoader} mechanism to automatically discover available factories and provides a
 * central place for obtaining actual {@link UnderFileSystem} instances.
 * </p>
 * <p>
 * Note that if you are bundling Alluxio plus your code in a shaded JAR using Maven, make sure to
 * use the {@code ServicesResourceTransformer} as otherwise your services file will override the
 * core provided services file and leave the standard factories and under file system
 * implementations unavailable.
 * </p>
 * @see ExtensionFactoryRegistry
 */
@NotThreadSafe
public final class UnderFileSystemFactoryRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemFactoryRegistry.class);
  private static final String UFS_EXTENSION_PATTERN = "alluxio-underfs-*.jar";
  private static ExtensionFactoryRegistry<UnderFileSystemFactory, UnderFileSystemConfiguration>
      sRegistryInstance;

  // prevent instantiation
  private UnderFileSystemFactoryRegistry() {}

  static {
    // Call the actual initializer which is a synchronized method for thread safety purposes
    init();
  }

  /**
   * Returns a read-only view of the available base factories.
   *
   * @return Read-only view of the available base factories
   */
  public static List<UnderFileSystemFactory> available() {
    return sRegistryInstance.getAvailable();
  }

  /**
   * Finds the first Under File System factory that supports the given path.
   *
   * @param path path
   * @param alluxioConf Alluxio configuration
   * @return factory if available, null otherwise
   */
  @Nullable
  public static UnderFileSystemFactory find(String path, AlluxioConfiguration alluxioConf) {
    return find(path, UnderFileSystemConfiguration.defaults(alluxioConf));
  }

  /**
   * Finds the first Under File System factory that supports the given path.
   *
   * @param path path
   * @param ufsConf configuration object for the UFS
   * @return factory if available, null otherwise
   */
  @Nullable
  public static UnderFileSystemFactory find(
      String path, UnderFileSystemConfiguration ufsConf) {
    List<UnderFileSystemFactory> factories = findAll(path, ufsConf);
    if (factories.isEmpty()) {
      LOG.warn("No Under File System Factory implementation supports the path {}. Please check if "
          + "the under storage path is valid.", path);
      return null;
    }
    LOG.debug("Selected Under File System Factory implementation {} for path {}",
        factories.get(0).getClass(), path);
    return factories.get(0);
  }

  /**
   * Finds all the Under File System factories that support the given path.
   *
   * @param path path
   * @param ufsConf configuration of the UFS
   * @return list of factories that support the given path which may be an empty list
   */
  public static List<UnderFileSystemFactory> findAll(String path,
      UnderFileSystemConfiguration ufsConf) {
    List<UnderFileSystemFactory> eligibleFactories = sRegistryInstance.findAll(path, ufsConf);
    if (eligibleFactories.isEmpty() && ufsConf.isSet(PropertyKey.UNDERFS_VERSION)) {
      String configuredVersion = ufsConf.get(PropertyKey.UNDERFS_VERSION);
      List<String> supportedVersions = getSupportedVersions(path, ufsConf);
      if (!supportedVersions.isEmpty()) {
        LOG.warn("Versions [{}] are supported for path {} but you have configured version: {}",
            StringUtils.join(supportedVersions, ","), path,
            configuredVersion);
      }
      ufsConf.set(PropertyKey.UNDERFS_VERSION, configuredVersion);
    }
    return eligibleFactories;
  }

  /**
   * Get a list of supported versions for a particular UFS path.
   *
   * @param path the UFS URI to test
   * @param ufsConf the UFS configuration for the mount
   * @return a list of supported versions. The list will be empty if the particular UFS type does
   *         not support setting a version on the mount.
   */
  public static List<String> getSupportedVersions(String path,
      UnderFileSystemConfiguration ufsConf) {
    // copy properties to not modify the original conf.
    UnderFileSystemConfiguration ufsConfCopy = UnderFileSystemConfiguration
        .defaults(new InstancedConfiguration(ufsConf.copyProperties()));
    // unset the configuration to make sure any supported factories for the path are returned.
    ufsConfCopy.unset(PropertyKey.UNDERFS_VERSION);
    // Check if any versioned factory supports the default configuration
    List<UnderFileSystemFactory> factories = sRegistryInstance.findAll(path, ufsConfCopy);
    List<String> supportedVersions = new ArrayList<>();
    for (UnderFileSystemFactory factory : factories) {
      if (!factory.getVersion().isEmpty()) {
        supportedVersions.add(factory.getVersion());
      }
    }
    return supportedVersions;
  }

  private static synchronized void init() {
    if (sRegistryInstance == null) {
      sRegistryInstance = new ExtensionFactoryRegistry<>(UnderFileSystemFactory.class,
          UFS_EXTENSION_PATTERN);
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
  public static void register(UnderFileSystemFactory factory) {
    sRegistryInstance.register(factory);
  }

  /**
   * Resets the registry to its default state
   * <p>
   * This clears the registry as it stands and rediscovers the available factories.
   * </p>
   */
  public static synchronized void reset() {
    sRegistryInstance.reset();
  }

  /**
   * Unregisters an existing factory.
   *
   * @param factory factory to unregister
   */
  public static void unregister(UnderFileSystemFactory factory) {
    sRegistryInstance.unregister(factory);
  }
}
