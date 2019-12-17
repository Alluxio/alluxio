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

  /** The type of this metric. */
  private final MetricType mMetricType;

  /** Whether the metric can be aggregated at cluster level. */
  private final boolean mIsClusterAggregated;

  /**
   * @param name name of this metric
   * @param description description of this metric
   * @param metricType the metric type of this metric
   * @param isClusterAggregated whether this metric can be aggregated at cluster level
   */
  private MetricKey(String name, String description,
      MetricType metricType, boolean isClusterAggregated) {
    mName = Preconditions.checkNotNull(name, "name");
    mDescription = Strings.isNullOrEmpty(description) ? "N/A" : description;
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
      MetricKey key = new MetricKey(mName, mDescription, mMetricType, mIsClusterAggregated);
      Preconditions.checkState(MetricKey.register(key), "Cannot register existing metric \"%s\"",
          mName);
      return key;
    }
  }
  // Master metrics

  // Worker metrics
  public static final MetricKey WORKER_BYTES_READ_ALLUXIO =
      new MetricKey.Builder(Name.WORKER_BYTES_READ_ALLUXIO)
          .setDescription("Total number of bytes read from Alluxio storage through this worker"
              + "This does not include UFS reads.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_ALLUXIO_THROUGHPUT =
      new MetricKey.Builder(Name.WORKER_BYTES_READ_ALLUXIO_THROUGHPUT)
          .setDescription("Bytes read throughput from Alluxio storage by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_ALLUXIO =
      new MetricKey.Builder(Name.WORKER_BYTES_WRITTEN_ALLUXIO)
          .setDescription("Total number of bytes written to Alluxio storage by this worker"
              + "This does not include UFS writes")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_ALLUXIO_THROUGHPUT =
      new MetricKey.Builder(Name.WORKER_BYTES_WRITTEN_ALLUXIO_THROUGHPUT)
          .setDescription("Bytes write throughput to Alluxio storage by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DOMAIN =
      new MetricKey.Builder(Name.WORKER_BYTES_READ_DOMAIN)
          .setDescription("Total number of bytes read from Alluxio storage"
              + "via domain socket by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DOMAIN_THROUGHPUT =
      new MetricKey.Builder(Name.WORKER_BYTES_READ_DOMAIN_THROUGHPUT)
          .setDescription("Bytes read throughput from Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DOMAIN =
      new MetricKey.Builder(Name.WORKER_BYTES_WRITTEN_DOMAIN)
          .setDescription("Total number of bytes written to Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT =
      new MetricKey.Builder(Name.WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT)
          .setDescription("Throughput of bytes written to Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggreagated(true)
          .build();

  public static final MetricKey WORKER_BYTES_READ_UFS =
      new MetricKey.Builder(Name.WORKER_BYTES_READ_UFS)
          .setDescription("Total number of bytes read from a specific Alluxio UFS by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_UFS_ALL =
      new MetricKey.Builder(Name.WORKER_BYTES_READ_UFS_ALL)
          .setDescription("Total number of bytes read from a all Alluxio UFSes by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_UFS_THROUGHPUT =
      new MetricKey.Builder(Name.WORKER_BYTES_READ_UFS_THROUGHPUT)
          .setDescription("Bytes read throughput from all Alluxio UFSes by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_UFS =
      new MetricKey.Builder(Name.WORKER_BYTES_WRITTEN_UFS)
          .setDescription("Total number of bytes written to a specific Alluxio UFS by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_UFS_ALL =
      new MetricKey.Builder(Name.WORKER_BYTES_WRITTEN_UFS_ALL)
          .setDescription("Total number of bytes written to all Alluxio UFSes by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_UFS_THROUGHPUT =
      new MetricKey.Builder(Name.WORKER_BYTES_WRITTEN_UFS_THROUGHPUT)
          .setDescription("Bytes write throughput to all Alluxio UFSes by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggreagated(true)
          .build();

  // Client metrics
  public static final MetricKey CLIENT_BYTES_READ_LOCAL =
      new MetricKey.Builder(Name.CLIENT_BYTES_READ_LOCAL)
          .setDescription("Total number of bytes short-circuit read from local storage "
              + "by this client")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggreagated(true)
          .build();

  public static final MetricKey CLIENT_BYTES_READ_LOCAL_THROUGHPUT =
      new MetricKey.Builder(Name.CLIENT_BYTES_READ_LOCAL_THROUGHPUT)
          .setDescription("Bytes throughput short-circuit read from local storage by this client")
          .setMetricType(MetricType.METER)
          .setIsClusterAggreagated(true)
          .build();

  public static final MetricKey CLIENT_BYTES_WRITTEN_UFS =
      new MetricKey.Builder(Name.CLIENT_BYTES_WRITTEN_UFS)
          .setDescription("Total number of bytes write to Alluxio UFS by this client")
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
    // Worker metrics
    public static final String WORKER_BYTES_READ_ALLUXIO = "WorkerBytesReadAlluxio";
    public static final String WORKER_BYTES_READ_ALLUXIO_THROUGHPUT
        = "WorkerBytesReadAlluxioThroughput";
    public static final String WORKER_BYTES_WRITTEN_ALLUXIO = "WorkerBytesWrittenAlluxio";
    public static final String WORKER_BYTES_WRITTEN_ALLUXIO_THROUGHPUT
        = "WorkerBytesWrittenAlluxioThroughput";
    public static final String WORKER_BYTES_READ_DOMAIN = "WorkerBytesReadDomain";
    public static final String WORKER_BYTES_READ_DOMAIN_THROUGHPUT
        = "WorkerBytesReadDomainThroughput";
    public static final String WORKER_BYTES_WRITTEN_DOMAIN = "WorkerBytesWrittenDomain";
    public static final String WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT
        = "WorkerBytesWrittenDomainThroughput";

    public static final String WORKER_BYTES_READ_UFS = "WorkerBytesReadPerUfs";
    public static final String WORKER_BYTES_READ_UFS_ALL = "WorkerBytesReadUfsAll";
    public static final String WORKER_BYTES_READ_UFS_THROUGHPUT = "WorkerBytesReadUfsThroughput";
    public static final String WORKER_BYTES_WRITTEN_UFS = "WorkerBytesWrittenPerUfs";
    public static final String WORKER_BYTES_WRITTEN_UFS_ALL = "WorkerBytesWrittenUfsAll";
    public static final String WORKER_BYTES_WRITTEN_UFS_THROUGHPUT
        = "WorkerBytesWrittenUfsThroughput";

    // Client metrics
    public static final String CLIENT_BYTES_READ_LOCAL = "ClientBytesReadLocal";
    public static final String CLIENT_BYTES_READ_LOCAL_THROUGHPUT
        = "ClientBytesReadLocalThroughput";
    public static final String CLIENT_BYTES_WRITTEN_UFS = "ClientBytesWrittenUfs";

    private Name() {} // prevent instantiation
  }
}
