package alluxio.master.block;

import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;

/**
 * This reviews the heap allocation status to decide whether a request should be
 * accepted.
 */
public class JvmSpaceReviewer {
  private static final Logger LOG = LoggerFactory.getLogger(JvmSpaceReviewer.class);

  // It is observed in tests that if a RegisterWorkerPRequest contains 1 million blocks,
  // processing the request will take 200M-400M heap allocation.
  // Here we use the upper bound to do the estimation.
  public static final int BLOCK_COUNT_MULTIPLIER = 400;

  private final MetricRegistry mMetricRegistry;

  JvmSpaceReviewer(MetricRegistry metricRegistry) {
    mMetricRegistry = metricRegistry;
  }

  public boolean reviewLeaseRequest(GetRegisterLeasePRequest request) {
    long blockCount = request.getBlockCount();
    long bytesAvailable = getAvailableBytes();
    long estimatedSpace = blockCount * BLOCK_COUNT_MULTIPLIER;
    if (bytesAvailable > estimatedSpace) {
      LOG.debug("{} bytes available on master. The register request with {} blocks is estimated to"
          + " need {} bytes. ", bytesAvailable, blockCount, estimatedSpace);
      return true;
    } else {
      LOG.info("{} bytes available on master. The register request with {} blocks is estimated to"
              + " need {} bytes. Rejected the request.", bytesAvailable, blockCount, estimatedSpace);
      return false;
    }
  }

  private long getAvailableBytes() {
    SortedMap<String, Gauge> heapGauges =  mMetricRegistry.getGauges(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.startsWith("heap.");
      }
    });

    // The values are in bytes
    long used = ((Gauge<Long>) heapGauges.get("heap.used")).getValue();
    long max = ((Gauge<Long>) heapGauges.get("heap.max")).getValue();

    return max - used;
  }
}
