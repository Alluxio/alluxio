package alluxio.master.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class RegisterLeaseManager {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterLeaseManager.class);

  private final Semaphore mSemaphore;

  Map<String, RegisterLeaseReviewer> mReviewerRegistry;
  // <WorkerId, ExpiryTime>
  Map<Long, RegisterLease> mOpenStreams;

  public RegisterLeaseManager() {
    int maxConcurrency = ServerConfiguration.global().getInt(PropertyKey.MASTER_REGISTER_MAX_CONCURRENCY);
    Preconditions.checkState(maxConcurrency > 0, "");
    mSemaphore = new Semaphore(maxConcurrency);

    mReviewerRegistry = new HashMap<>();
    mReviewerRegistry.put(JvmSpaceReviewer.class.getName(), new JvmSpaceReviewer());

    mOpenStreams = new ConcurrentHashMap();
  }

  // If the lease exists, it will be returned
  public Optional<RegisterLease> tryAcquireLease(GetRegisterLeasePRequest request) {
    long workerId = request.getWorkerId();
    if (mOpenStreams.containsKey(workerId)) {
      LOG.info("Found existing lease for worker {}", workerId);
      return Optional.of(mOpenStreams.get(workerId));
    }

    // If any of the reviewer rejects, the request will be rejected
    for (Map.Entry<String, RegisterLeaseReviewer> entry : mReviewerRegistry.entrySet()) {
      if (!entry.getValue().reviewLeaseRequest(request)) {
        return Optional.empty();
      }
    }

    // Check for expired leases here instead of having a separate thread
    tryRecycleLease();

    if (mSemaphore.tryAcquire()) {
      RegisterLease lease = new RegisterLease();
      mOpenStreams.put(request.getWorkerId(), lease);
      return Optional.of(lease);
    }

    return Optional.empty();
  }

  public void tryRecycleLease() {
    long now = CommonUtils.getCurrentMs();

    mOpenStreams.entrySet().removeIf(entry -> {
      if (entry.getValue().mExpireTime.toEpochMilli() > now) {
        LOG.info("Recycled expired lease for worker {}", entry.getKey());
        return true;
      }
      return false;
    });
  }

  public boolean checkLease(long workerId) {
    return mOpenStreams.containsKey(workerId);
  }

  public void releaseLease(long workerId) {
    if (mOpenStreams.containsKey(workerId)) {
      mSemaphore.release();
    }
  }
}
