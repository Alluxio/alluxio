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

import alluxio.grpc.MetricType;

import alluxio.util.CommonUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A metric of a given instance. The instance can be master, worker, or client.
 */
public final class Metric implements Serializable {
  private static final long serialVersionUID = -2236393414222298333L;

  public static final String TAG_SEPARATOR = ":";
  private static final ConcurrentHashMap<UserMetricKey, String> CACHED_METRICS =
      new ConcurrentHashMap<>();

  private final MetricsSystem.InstanceType mInstanceType;
  private final String mSource;
  private final String mName;
  private final MetricType mMetricType;
  // TODO(yupeng): consider a dedicated data structure for tag, when more functionality are added to
  // tags in the future
  private final Map<String, String> mTags;

  /**
   * The unique identifier to represent this metric.
   * The pattern is instance.name[.tagName:tagValue]*[.source].
   * Fetched once and assumed to be immutable.
   */
  private final Supplier<String> mFullMetricNameSupplier =
      CommonUtils.memoize(this::constructFullMetricName);

  private AtomicDouble mValue;

  /**
   * Constructs a {@link Metric} instance.
   *
   * @param instanceType the instance type
   * @param source the metric source
   * @param metricType the type of the metric
   * @param name the metric name
   * @param value the value
   */
  public Metric(MetricsSystem.InstanceType instanceType, String source,
      MetricType metricType, String name, Double value) {
    Preconditions.checkNotNull(name, "name");
    mInstanceType = instanceType;
    mSource = source;
    mMetricType = metricType;
    mName = name;
    mValue = new AtomicDouble(value);
    mTags = new LinkedHashMap<>();
  }

  /**
   * Add metric value delta to the existing value.
   * This method should only be used by {@link alluxio.master.metrics.MetricsStore}
   *
   * @param delta value to add
   */
  public void addValue(double delta) {
    mValue.addAndGet(delta);
  }

  /**
   * Set the metric value.
   * This method should only be used by {@link alluxio.master.metrics.MetricsStore}
   *
   * @param value value to set
   */
  public void setValue(double value) {
    mValue.set(value);
  }

  /**
   * @return the metric value
   */
  public double getValue() {
    return mValue.get();
  }

  /**
   * @return the instance type
   */
  public MetricsSystem.InstanceType getInstanceType() {
    return mInstanceType;
  }

  /**
   * Adds a new tag. If the tag name already exists, it will be replaced.
   *
   * @param name the tag name
   * @param value the tag value
   */
  public void addTag(String name, String value) {
    mTags.put(name, value);
  }

  /**
   * @return the metric source
   */
  public String getSource() {
    return mSource;
  }

  /**
   * @return the metric type
   */
  public MetricType getMetricType() {
    return mMetricType;
  }

  /**
   * @return the metric name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the tags
   */
  public Map<String, String> getTags() {
    return mTags;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof Metric)) {
      return false;
    }
    Metric metric = (Metric) other;
    return Objects.equal(getFullMetricName(), metric.getFullMetricName())
        && Objects.equal(mValue.get(), metric.mValue.get());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getFullMetricName(), mValue.get());
  }

  /**
   * @return the fully qualified metric name, which is of pattern
   *         instance.name[.tagName:tagValue]*[.source], where the tags are appended
   *         at the end
   */
  public String getFullMetricName() {
    return mFullMetricNameSupplier.get();
  }

  /**
   * @return the fully qualified metric name, which is of pattern
   *         instance.name[.tagName:tagValue]*[.source], where the tags are appended
   *         at the end
   */
  private String constructFullMetricName() {
    StringBuilder sb = new StringBuilder();
    sb.append(mInstanceType).append('.');
    sb.append(mName);
    for (Entry<String, String> entry : mTags.entrySet()) {
      sb.append('.').append(entry.getKey()).append(TAG_SEPARATOR).append(entry.getValue());
    }
    if (mSource != null) {
      sb.append('.');
      sb.append(mSource);
    }
    return sb.toString();
  }

  /**
   * @return the proto object it converts to. Note the value must be either integer or long
   */
  public alluxio.grpc.Metric toProto() {
    alluxio.grpc.Metric.Builder metric = alluxio.grpc.Metric.newBuilder();
    metric.setInstance(mInstanceType.toString()).setMetricType(mMetricType)
        .setName(mName).setValue(mValue.get()).putAllTags(mTags);

    if (!mSource.isEmpty()) {
      metric.setSource(mSource);
    }

    return metric.build();
  }

  /**
   * Gets the metric name with the appendix of tags. The returned name is of the pattern
   * name[.tagName:tagValue]*.
   *
   * @param name the metric name
   * @param tags the tag name and tag value pairs
   * @return the name with the tags appended
   */
  public static String getMetricNameWithTags(String name, String... tags) {
    Preconditions.checkArgument(tags.length % 2 == 0,
        "The tag arguments should be tag name and tag value pairs");
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    for (int i = 0; i < tags.length; i += 2) {
      sb.append('.').append(tags[i]).append(TAG_SEPARATOR).append(tags[i + 1]);
    }
    return sb.toString();
  }

  /**
   * Gets a metric name with a specific user tag.
   *
   * @param metricName the name of the metric
   * @param userName the user
   * @return a metric name with the user tagged
   */
  public static String getMetricNameWithUserTag(String metricName, String userName) {
    UserMetricKey k = new UserMetricKey(metricName, userName);
    String result = CACHED_METRICS.get(k);
    if (result != null) {
      return result;
    }
    return CACHED_METRICS.computeIfAbsent(k, key -> metricName + "." + MetricInfo.TAG_USER
        + TAG_SEPARATOR + userName);
  }

  /**
   * Gets value of ufs tag from the full metric name.
   *
   * @param fullName the full metric name
   * @return value of ufs tag
   */
  public static String getTagUfsValueFromFullName(String fullName) {
    String[] pieces = fullName.split("\\.");
    if (pieces.length < 3) {
      return null;
    }
    for (int i = 2; i < pieces.length; i++) {
      String current = pieces[i];
      if (current.contains(TAG_SEPARATOR) && current.contains(MetricInfo.TAG_UFS)) {
        return current.substring(current.indexOf(TAG_SEPARATOR) + 1);
      }
    }
    return null;
  }

  /**
   * Gets the simple name without the tags.
   *
   * @param fullName the full metric name
   * @return the base name
   */
  public static String getBaseName(String fullName) {
    String[] pieces = fullName.split("\\.");
    if (pieces.length < 2) {
      return fullName;
    } else {
      return pieces[0] + "." + pieces[1];
    }
  }

  /**
   * Creates the metric from the full name and the value.
   *
   * @param fullName the full name
   * @param value the value
   * @param metricType the type of metric that is being created
   * @return the created metric
   */
  public static Metric from(String fullName, double value, MetricType metricType) {
    String[] pieces = fullName.split("\\.");
    Preconditions.checkArgument(pieces.length > 1, "Incorrect metrics name: %s.", fullName);
    int len = pieces.length;
    String source = null;
    int tagEndIndex = len;
    // Master or cluster metrics don't have source included.
    if (!pieces[0].equals(MetricsSystem.InstanceType.MASTER.toString())
        && !pieces[0].equals(MetricsSystem.CLUSTER)) {
      source = pieces[len - 1];
      tagEndIndex = len - 1;
    }
    MetricsSystem.InstanceType instance = MetricsSystem.InstanceType.fromString(pieces[0]);
    // pieces[1] refer to metric name
    Metric metric = new Metric(instance, source, metricType, pieces[1], value);

    // parse tags
    for (int i = 2; i < tagEndIndex; i++) {
      String tagStr = pieces[i];
      if (!tagStr.contains(TAG_SEPARATOR)) {
        // Unknown tag
        continue;
      }
      int tagSeparatorIdx = tagStr.indexOf(TAG_SEPARATOR);
      metric.addTag(tagStr.substring(0, tagSeparatorIdx), tagStr.substring(tagSeparatorIdx + 1));
    }
    return metric;
  }

  /**
   * Constructs the metric object from the proto format.
   *
   * @param metric the metric in proto format
   * @return the constructed metric
   */
  public static Metric fromProto(alluxio.grpc.Metric metric) {
    Metric created = new Metric(
        MetricsSystem.InstanceType.fromString(metric.getInstance()),
        metric.getSource(),
        metric.getMetricType(),
        metric.getName(),
        metric.getValue());
    for (Entry<String, String> entry : metric.getTagsMap().entrySet()) {
      created.addTag(entry.getKey(), entry.getValue());
    }
    return created;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", mSource)
        .add("instanceType", mInstanceType)
        .add("metricType", mMetricType)
        .add("name", mName)
        .add("tags", mTags)
        .add("value", mValue.get())
        .toString();
  }

  /**
   * Data structure representing a metric name and user name.
   */
  private static class UserMetricKey {
    private String mMetric;
    private String mUser;

    private UserMetricKey(String metricName, String userName) {
      mMetric = metricName;
      mUser = userName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof UserMetricKey)) {
        return false;
      }
      UserMetricKey that = (UserMetricKey) o;
      return Objects.equal(mMetric, that.mMetric)
          && Objects.equal(mUser, that.mUser);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mMetric, mUser);
    }
  }
}
