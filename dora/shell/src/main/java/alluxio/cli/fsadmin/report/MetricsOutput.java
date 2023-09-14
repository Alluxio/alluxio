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

package alluxio.cli.fsadmin.report;

import alluxio.grpc.MetricValue;
import alluxio.metrics.MetricsSystem;
import alluxio.util.FormatUtils;

import com.google.common.math.DoubleMath;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * An output class, describing the metrics.
 */
public class MetricsOutput {
  private static final String BYTES_METRIC_IDENTIFIER = "Bytes";
  private static final String THROUGHPUT_METRIC_IDENTIFIER = "Throughput";
  private static final DecimalFormat DECIMAL_FORMAT
      = new DecimalFormat("###,###.#####", new DecimalFormatSymbols(Locale.US));
  private List<SerializableMetricInfo> mMetricsInfo;

  private class SerializableMetricInfo {
    private static final String PER_MINUTE = "/MIN";
    private String mKey;
    private String mType;
    private String mValue;

    public SerializableMetricInfo(Map.Entry<String, MetricValue> entry) {
      mKey = entry.getKey();
      mType = entry.getValue().getMetricType().toString();
      if (entry.getValue().hasStringValue()) {
        mValue = entry.getValue().getStringValue();
      } else {
        double doubleValue = entry.getValue().getDoubleValue();
        if (mKey.contains(BYTES_METRIC_IDENTIFIER)) {
          // Bytes long can be transformed to human-readable format
          mValue = FormatUtils.getSizeFromBytes((long) doubleValue);
          if (mKey.contains(THROUGHPUT_METRIC_IDENTIFIER)) {
            mValue = mValue + PER_MINUTE;
          }
        } else if (DoubleMath.isMathematicalInteger(doubleValue)) {
          mValue = DECIMAL_FORMAT.format((long) doubleValue);
        } else {
          mValue = String.valueOf(doubleValue);
        }
      }
    }

    public String getKey() {
      return mKey;
    }

    public void setKey(String key) {
      mKey = key;
    }

    public String getType() {
      return mType;
    }

    public void setType(String type) {
      mType = type;
    }

    public String getValue() {
      return mValue;
    }

    public void setValue(String value) {
      mValue = value;
    }
  }

  /**
   * Creates a new instance of {@link MetricsOutput}.
   *
   * @param metrics metric to parse output from
   */
  public MetricsOutput(Map<String, MetricValue> metrics) {
    mMetricsInfo = new ArrayList<>();
    for (Map.Entry<String, MetricValue> entry : metrics.entrySet()) {
      String key = entry.getKey();
      if (!isAlluxioMetric(key)) {
        continue;
      }
      mMetricsInfo.add(new SerializableMetricInfo(entry));
    }
  }

  /**
   * Checks if a metric is Alluxio metric.
   *
   * @param name name of the metrics to check
   * @return true if a metric is an Alluxio metric, false otherwise
   */
  private boolean isAlluxioMetric(String name) {
    for (MetricsSystem.InstanceType instance : MetricsSystem.InstanceType.values()) {
      if (name.startsWith(instance.toString())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get metrics info.
   * @return metrics info
   */
  public List<SerializableMetricInfo> getMetricsInfo() {
    return mMetricsInfo;
  }

  /**
   * Set metrics info.
   * @param metricsInfo metrics info
   */
  public void setMetricsInfo(List<SerializableMetricInfo> metricsInfo) {
    mMetricsInfo = metricsInfo;
  }
}
