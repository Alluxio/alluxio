package alluxio.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.HashMap;
import java.util.Map;

/**
 * A set of counters for the log metric.
 */
public class LogStateCounterSet implements MetricSet {
    @Override
    public Map<String, Metric> getMetrics() {
        final Map<String, Metric> gauges = new HashMap<>();
        gauges.put("log.info.count", (Gauge<Long>) EventCounter::getInfo);
        gauges.put("log.warn.count", (Gauge<Long>) EventCounter::getWarn);
        gauges.put("log.error.count", (Gauge<Long>) EventCounter::getError);
        gauges.put("log.fatal.count", (Gauge<Long>) EventCounter::getFatal);
        return gauges;
    }
}
