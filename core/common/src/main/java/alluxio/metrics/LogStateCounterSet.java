package alluxio.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.RatioGauge;

import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

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
