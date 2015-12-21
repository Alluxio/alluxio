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

package tachyon.client.file.policy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.client.block.BlockWorkerInfo;

/**
 * <p>
 * Central registry of available {@link FileWriteLocationPolicyFactory} instances that uses the
 * {@link ServiceLoader} mechanism to automatically discover available factories and provides a
 * central place for obtaining actual {@link FileWriteLocationPolicy} instances.
 * </p>
 * <h3>Registering New Factories</h3>
 * <p>
 * New factories can be registered either using the {@linkplain ServiceLoader} based automatic
 * discovery mechanism or manually using the static
 * {@link #register(FileWriteLocationPolicyFactory)} method. The down-side of the automatic
 * discovery mechanism is that the discovery order is not controllable. As a result if your
 * implementation is designed as a replacement for one of the standard implementations, depending on
 * the order in which the JVM discovers the services your own implementation may not take priority.
 * You can enable {@code DEBUG} level logging for this class to see the order in which factories are
 * discovered and which is selected when obtaining a {@link FileWriteLocationPolicyFactory} instance
 * for a path. If this shows that your implementation is not getting discovered or used, you may
 * wish to use the manual registration approach.
 * </p>
 * <h4>Automatic Discovery</h4>
 * <p>
 * To use the {@linkplain ServiceLoader} based mechanism you need to have a file named
 * {@code tachyon.client.file.policy.FileWriteLocationPolicyFactory} placed in the
 * {@code META-INF\services} directory of your project. This file should contain the full name of
 * your factory types (one per line), your factory types must have a public unparameterised
 * constructor available (see {@link ServiceLoader} for more detail on this). You can enable
 * {@code DEBUG} level logging to see factories as they are discovered if you wish to check that
 * your implementation gets discovered.
 * </p>
 * <p>
 * Note that if you are bundling Tachyon plus your code in a shaded JAR using Maven, make sure to
 * use the {@code ServicesResourceTransformer} as otherwise your services file will override the
 * core provided services file and leave the standard factories unavailable.
 * </p>
 * <h4>Manual Registration</h4>
 * <p>
 * To manually register a factory, simply pass an instance of your factory to the
 * {@link #register(FileWriteLocationPolicyFactory)} method. This can be useful when your factory
 * cannot be instantiated without arguments or in cases where automatic discovery does not give your
 * factory priority. Factories registered this way will be registered at the start of the factories
 * list so will have the first opportunity to indicate whether they support a requested path.
 * </p>
 * <h4>Obtain a policy</h4>
 * <p>
 * </p>
 */
@SuppressWarnings("rawtypes")
public final class LocationPolicyRegistry {
  private static final Map<Class<? extends FileWriteLocationPolicy>, FileWriteLocationPolicyFactory> FACTORIES =
      Maps.newHashMap();
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static boolean sInit = false;

  static {
    // Call the actual initializer which is a synchronized method for thread safety purposes
    init();
  }


  public static Set<Class<? extends FileWriteLocationPolicy>> availablePolices() {
    return Collections.unmodifiableSet(FACTORIES.keySet());
  }

  private static synchronized void init() {
    if (sInit) {
      return;
    }

    // Discover and register the available factories
    ServiceLoader<FileWriteLocationPolicyFactory> discoveredFactories =
        ServiceLoader.load(FileWriteLocationPolicyFactory.class);
    for (FileWriteLocationPolicyFactory<?> factory : discoveredFactories) {
      LOG.debug("Discovered file-write location policy factory {}", factory.getClass());
      FACTORIES.put(factory.getPolicyClass(), factory);
    }

    sInit = true;
  }

  /**
   * Creates an instance of the specified location policy with the configurations.
   *
   * @param policyClass the class of the location policy
   * @param workerInfoList list of the active workers information
   * @param options the configuration of the policy
   * @return a new instance of the location policy * @throws IllegalArgumentException Thrown if
   *         there is no location policy found for the policy class
   */
  @SuppressWarnings("unchecked")
  public static <T extends FileWriteLocationPolicy> T create(Class<T> policyClass,
      List<BlockWorkerInfo> workerInfoList, FileWriteLocationPolicyOptions options) {
    Preconditions.checkArgument(FACTORIES.containsKey(policyClass),
        "No file write location policy found for " + policyClass);
    return (T) FACTORIES.get(policyClass).create(workerInfoList, options);
  }

  /**
   * Registers a new factory
   * <p>
   * Factories are registered at the start of the factories list so they can override the existing
   * automatically discovered factories. Generally if you use the {@link ServiceLoader} mechanism
   * properly it should be unnecessary to call this, however since ServiceLoader discovery order may
   * be susceptible to class loader behavioral differences there may be rare cases when you need to
   * manually register the desired factory.
   * </p>
   *
   * @param factory Factory to register
   */
  public static void register(FileWriteLocationPolicyFactory<?> factory) {
    if (factory == null) {
      return;
    }

    LOG.debug("Registered file-write location policy factory {}", factory.getClass());
    FACTORIES.put(factory.getPolicyClass(), factory);
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
  public static void unregister(FileWriteLocationPolicyFactory<?> factory) {
    if (factory == null) {
      return;
    }

    LOG.debug("Unregistered file-write location policy factory {}", factory.getClass());
    FACTORIES.remove(factory.getClass());
  }
}
