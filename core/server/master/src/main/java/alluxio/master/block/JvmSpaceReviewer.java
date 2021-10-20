package alluxio.master.block;

import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;

/**
 * This reviews the heap allocation status to decide whether a request should be
 * accepted.
 */
public class JvmSpaceReviewer implements RegisterLeaseReviewer {
  private static final Logger LOG = LoggerFactory.getLogger(JvmSpaceReviewer.class);

  // It is observed in tests that if a RegisterWorkerPRequest contains 1 million blocks,
  // processing the request will take 200M-400M heap allocation.
  // Here we use the upper bound to do the estimation.
  private static final int BLOCK_COUNT_MULTIPLIER = 400;

  JvmSpaceReviewer() {}

  @Override
  public boolean reviewLeaseRequest(GetRegisterLeasePRequest request) {
    long blockCount = request.getBlockCount();
    long bytesAvailable = getAvailableBytes();
    long estimatedSpace = blockCount * BLOCK_COUNT_MULTIPLIER;
    if (bytesAvailable > estimatedSpace) {
//      LOG.info("{} bytes available on master. The register request with {} blocks is estimated to"
//          + " need {} bytes. Review passed.", bytesAvailable, blockCount, estimatedSpace);
      return true;
    } else {
      LOG.info("{} bytes available on master. The register request with {} blocks is estimated to"
              + " need {} bytes. Review rejected.", bytesAvailable, blockCount, estimatedSpace);
      return false;
    }
  }

  public static long getAvailableBytes() {
    SortedMap<String, Gauge> heapGauges =  MetricsSystem.METRIC_REGISTRY.getGauges(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.startsWith("heap.");
      }
    });

    // calculate space available on the heap
    long used = ((Gauge<Long>) heapGauges.get("heap.used")).getValue();
    long max = ((Gauge<Long>) heapGauges.get("heap.max")).getValue();

    return max - used;
  }
}
