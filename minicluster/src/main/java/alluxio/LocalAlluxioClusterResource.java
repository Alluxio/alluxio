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

package alluxio;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.LocalAlluxioCluster;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticatedClientUser;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A local alluxio cluster.
 */
@NotThreadSafe
public final class LocalAlluxioClusterResource {

  /** Number of Alluxio workers in the cluster. */
  private final int mNumWorkers;

  /** Weather to include the secondary master. */
  private final boolean mIncludeSecondary;

  /** Weather to include the proxy. */
  private final boolean mIncludeProxy;

  /** ServerConfiguration values for the cluster. */
  private final Map<PropertyKey, Object> mConfiguration = new HashMap<>();

  /** The Alluxio cluster being managed. */
  private LocalAlluxioCluster mLocalAlluxioCluster = null;

  /** The name of the cluster. */
  private static final String CLUSTER_NAME = "singleProcess";

  /**
   * Creates a new instance.
   *
   * @param numWorkers the number of Alluxio workers to launch
   * @param configuration configuration for configuring the cluster
   */
  private LocalAlluxioClusterResource(boolean includeSecondary,
      boolean includeProxy, int numWorkers, Map<PropertyKey, Object> configuration) {
    mIncludeSecondary = includeSecondary;
    mIncludeProxy = includeProxy;
    mNumWorkers = numWorkers;
    mConfiguration.putAll(configuration);
    MetricsSystem.resetCountersAndGauges();
  }

  /**
   * @return the {@link LocalAlluxioCluster} being managed
   */
  public LocalAlluxioCluster get() {
    return mLocalAlluxioCluster;
  }

  /**
   * Adds a property to the cluster resource. Unset a property by passing a null value.
   *
   * @param key property key
   * @param value property value
   * @return the cluster resource
   */
  public LocalAlluxioClusterResource setProperty(PropertyKey key, Object value) {
    if (value == null) {
      mConfiguration.remove(key);
    } else {
      mConfiguration.put(key, value);
    }
    return this;
  }

  /**
   * Explicitly starts the {@link LocalAlluxioCluster}.
   */
  public void start() throws Exception {
    AuthenticatedClientUser.remove();
    // Create a new cluster.
    mLocalAlluxioCluster = new LocalAlluxioCluster(mNumWorkers, mIncludeSecondary, mIncludeProxy);
    // Init configuration
    mLocalAlluxioCluster.initConfiguration(CLUSTER_NAME);
    // Overwrite the configuration with the specific parameters
    for (Entry<PropertyKey, Object> entry : mConfiguration.entrySet()) {
      ServerConfiguration.set(entry.getKey(), entry.getValue());
    }
    ServerConfiguration.global().validate();
    // Start the cluster
    mLocalAlluxioCluster.start();
  }

  /**
   * Explicitly stops the {@link LocalAlluxioCluster}.
   */
  public void stop() throws Exception {
    mLocalAlluxioCluster.stop();
  }

  /**
   * Builder for a {@link LocalAlluxioClusterResource}.
   */
  public static class Builder {
    private boolean mIncludeSecondary;
    private boolean mIncludeProxy;
    private int mNumWorkers;
    private Map<PropertyKey, Object> mConfiguration;

    /**
     * Constructs the builder with default values.
     */
    public Builder() {
      mIncludeSecondary = false;
      mIncludeProxy = false;
      mNumWorkers = 1;
      mConfiguration = new HashMap<>();
    }

    /**
     * @param includeSecondary whether to include the secondary master
     * @return the updated builder
     */
    public Builder setIncludeSecondary(boolean includeSecondary) {
      mIncludeSecondary = includeSecondary;
      return this;
    }

    /**
     * @param includeProxy whether to include the proxy
     * @return the updated builder
     */
    public Builder setIncludeProxy(boolean includeProxy) {
      mIncludeProxy = includeProxy;
      return this;
    }

    /**
     * @param numWorkers the number of workers to run in the cluster
     * @return the updated builder
     */
    public Builder setNumWorkers(int numWorkers) {
      mNumWorkers = numWorkers;
      return this;
    }

    /**
     * @param key the property key to set for the cluster
     * @param value the value to set it to
     * @return the updated builder
     */
    public Builder setProperty(PropertyKey key, Object value) {
      mConfiguration.put(key, value);
      return this;
    }

    /**
     * @return a {@link LocalAlluxioClusterResource} for the current builder values
     */
    public LocalAlluxioClusterResource build() {
      return new LocalAlluxioClusterResource(mIncludeSecondary, mIncludeProxy,
          mNumWorkers, mConfiguration);
    }
  }
}
