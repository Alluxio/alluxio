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

package alluxio.metrics;

import alluxio.exception.ExceptionMessage;
import alluxio.grpc.MetricType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric keys. This class provides a set of pre-defined Alluxio metric keys.
 */
@ThreadSafe
public final class MetricKey implements Comparable<MetricKey> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricKey.class);

  /**
   * A map from default metric key's string name to the metric.
   * This map must be the first to initialize within this file.
   */
  private static final Map<String, MetricKey> METRIC_KEYS_MAP = new ConcurrentHashMap<>();

  /** Metric name. */
  private final String mName;

  /** Metric Key description. */
  private final String mDescription;

  /** The instance type this metric belongs to. */
  private final MetricsSystem.InstanceType mInstanceType;

  /** The type of this metric. */
  private final MetricType mMetricType;

  /** Whether the metric can be aggregated at cluster level. */
  private final boolean mIsClusterAggregated;

  /**
   * @param name name of this metric
   * @param description description of this metric
   * @param instanceType the instance type this metric belongs to
   * @param metricType the metric type of this metric
   * @param isClusterAggregated whether this metric can be aggregated at cluster level
   */
  private MetricKey(String name, String description, MetricsSystem.InstanceType instanceType,
      MetricType metricType, boolean isClusterAggregated) {
    mName = Preconditions.checkNotNull(name, "name");
    mDescription = Strings.isNullOrEmpty(description) ? "N/A" : description;
    mInstanceType = Preconditions.checkNotNull(instanceType, "instance type");
    mMetricType = metricType;
    mIsClusterAggregated = isClusterAggregated;
  }

  /**
   * @param name name of a metric
   * @return whether the given name is a valid Metric name
   */
  public static boolean isValid(String name) {
    return METRIC_KEYS_MAP.containsKey(name);
  }

  /**
   * Parses a given name and return its corresponding {@link MetricKey},
   * throwing exception if no such a Metric can be found.
   *
   * @param name name of the Metric key
   * @return corresponding Metric
   */
  public static MetricKey fromString(String name) {
    MetricKey key = METRIC_KEYS_MAP.get(name);
    if (key != null) {
      return key;
    }
    throw new IllegalArgumentException(ExceptionMessage.INVALID_METRIC_KEY.getMessage(name));
  }

  /**
   * @return all pre-defined Alluxio metric keys
   */
  public static Collection<? extends MetricKey> allMetricKeys() {
    return Sets.newHashSet(METRIC_KEYS_MAP.values());
  }

  /**
   * @return the name of the Metric
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the description of a Metric
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * @return the description of a Metric
   */
  public MetricsSystem.InstanceType getInstanceType() {
    return mInstanceType;
  }

  /**
   * @return the metric type of a Metric
   */
  public MetricType getMetricType() {
    return mMetricType;
  }

  /**
   * @return true if this metrics can be aggregated at cluster level
   */
  public boolean isClusterAggregated() {
    return mIsClusterAggregated;
  }

  /**
   * Builder to create {@link MetricKey} instances. Note that, <code>Builder.build()</code> will
   * throw exception if there is an existing Metric built with the same name.
   */
  public static final class Builder {
    private String mName;
    private String mDescription;
    private boolean mIsClusterAggregated;
    private MetricsSystem.InstanceType mInstanceType;
    private MetricType mMetricType = MetricType.GAUGE;

    /**
     * @param name name of the Metric
     */
    public Builder(String name) {
      mName = name;
    }

    /**
     * @param name name for the Metric
     * @return the updated builder instance
     */
    public MetricKey.Builder setName(String name) {
      mName = name;
      return this;
    }

    /**
     * @param description of the Metric
     * @return the updated builder instance
     */
    public MetricKey.Builder setDescription(String description) {
      mDescription = description;
      return this;
    }

    /**
     * @param isClusterAggreagated whether this metric can be aggregated at cluster level
     * @return the updated builder instance
     */
    public MetricKey.Builder setIsClusterAggreagated(boolean isClusterAggreagated) {
      mIsClusterAggregated = isClusterAggreagated;
      return this;
    }

    /**
     * @param instanceType the instance type this metric belongs to
     * @return the updated builder instance
     */
    public MetricKey.Builder setInstanceType(MetricsSystem.InstanceType instanceType) {
      mInstanceType = instanceType;
      return this;
    }

    /**
     * @param metricType the metric type of this metric
     * @return the updated builder instance
     */
    public MetricKey.Builder setMetricType(MetricType metricType) {
      mMetricType = metricType;
      return this;
    }

    /**
     * Creates and registers the Metric key.
     *
     * @return the created Metric key instance
     */
    public MetricKey build() {
      MetricKey key = new MetricKey(mName, mDescription,
          mInstanceType, mMetricType, mIsClusterAggregated);
      Preconditions.checkState(MetricKey.register(key), "Cannot register existing metric \"%s\"",
          mName);
      return key;
    }
  }

  // Client metrics
  public static final MetricKey BYTES_READ_LOCAL =
      new MetricKey.Builder(Name.BYTES_READ_LOCAL)
          .setDescription("Total number of bytes short-circuit read from local storage.")
          .setInstanceType(MetricsSystem.InstanceType.CLIENT)
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();

  public static final MetricKey BYTES_READ_LOCAL_THROUGHPUT =
      new MetricKey.Builder(Name.BYTES_READ_LOCAL_THROUGHPUT)
          .setDescription("Bytes throughput short-circuit read from local storage.")
          .setInstanceType(MetricsSystem.InstanceType.CLIENT)
          .setMetricType(MetricType.METER)
          .setIsClusterAggreagated(true)
          .build();

  public static final MetricKey BYTES_WRITTEN_UFS =
      new MetricKey.Builder(Name.BYTES_WRITTEN_UFS)
          .setDescription("Total number of bytes write to Alluxio UFS.")
          .setInstanceType(MetricsSystem.InstanceType.CLIENT)
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();

  /**
   * Registers the given key to the global key map.
   *
   * @param key the Metric key
   * @return whether the Metric key is successfully registered
   */
  @VisibleForTesting
  public static boolean register(MetricKey key) {
    String name = key.getName();
    if (METRIC_KEYS_MAP.containsKey(name)) {
      return false;
    }

    METRIC_KEYS_MAP.put(name, key);
    return true;
  }

  /**
   * Unregisters the given key from the global key map.
   *
   * @param key the Metric to unregister
   */
  @VisibleForTesting
  public static void unregister(MetricKey key) {
    METRIC_KEYS_MAP.remove(key.getName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetricKey)) {
      return false;
    }
    MetricKey that = (MetricKey) o;
    return Objects.equal(mName, that.mName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName);
  }

  @Override
  public String toString() {
    return mName;
  }

  @Override
  public int compareTo(MetricKey o) {
    return mName.compareTo(o.mName);
  }

  /**
   * A nested class to hold named string constants for their corresponding metrics.
   */
  @ThreadSafe
  public static final class Name {
    // Client metrics
    public static final String BYTES_READ_LOCAL = "BytesReadLocal";
    public static final String BYTES_READ_LOCAL_THROUGHPUT = "BytesReadLocalThroughput";
    public static final String BYTES_WRITTEN_UFS = "BytesWrittenUfs";

    private Name() {} // prevent instantiation
  }
}
