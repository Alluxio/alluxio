package alluxio.cli.fsadmin.report;

import alluxio.grpc.MetricValue;
import alluxio.metrics.MetricsSystem;
import alluxio.util.FormatUtils;
import com.fasterxml.jackson.core.SerializableString;
import com.google.common.math.DoubleMath;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MetricsOutput {
    private static final String BYTES_METRIC_IDENTIFIER = "Bytes";
    private static final String THROUGHPUT_METRIC_IDENTIFIER = "Throughput";
    private static final DecimalFormat DECIMAL_FORMAT
        = new DecimalFormat("###,###.#####", new DecimalFormatSymbols(Locale.US));
    private List<SerializableMetricInfo> mMetricsInfo;

    private class SerializableMetricInfo {
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
                        mValue = mValue + "/MIN";
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

    public MetricsOutput(Map<String, MetricValue> metrics){
        mMetricsInfo = new ArrayList<>();
        for (Map.Entry<String, MetricValue> entry : metrics.entrySet()){
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

    public List<SerializableMetricInfo> getMetricsInfo() {
        return mMetricsInfo;
    }

    public void setMetricsInfo(List<SerializableMetricInfo> metricsInfo) {
        mMetricsInfo = metricsInfo;
    }

}
