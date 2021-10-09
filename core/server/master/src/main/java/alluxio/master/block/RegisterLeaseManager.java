package alluxio.master.block;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Gauge;
import com.google.common.base.Preconditions;

import java.util.Optional;
import java.util.concurrent.Semaphore;

public class RegisterLeaseManager {
  private final Semaphore mSemaphore;

  public RegisterLeaseManager() {
    int maxConcurrency = ServerConfiguration.global().getInt(PropertyKey.MASTER_REGISTER_MAX_CONCURRENCY);
    Preconditions.checkState(maxConcurrency > 0, "");
    mSemaphore = new Semaphore(maxConcurrency);
  }

  // TODO(jiacheng): Associate the lease with the workerId and recycle
  public Optional<RegisterLease> tryAcquireLease() {
    // TODO(jiacheng): consider concurrency
    if (mSemaphore.tryAcquire()) {
      return Optional.of(new RegisterLease());
    }

    return Optional.empty();
    // TODO(jiacheng): consider heap
  }

  public void releaseLease() {
    mSemaphore.release();
  }

  public static HeapUsage getHeapUsage() {
    Gauge<Long> init = (Gauge<Long>) MetricsSystem.METRIC_REGISTRY.getGauges().get("heap.init");
    Gauge<Long> used = (Gauge<Long>) MetricsSystem.METRIC_REGISTRY.getGauges().get("heap.used");
    Gauge<Long> max = (Gauge<Long>) MetricsSystem.METRIC_REGISTRY.getGauges().get("heap.max");
    Gauge<Long> committed = (Gauge<Long>) MetricsSystem.METRIC_REGISTRY.getGauges().get("heap.committed");
    return new HeapUsage(init.getValue(), used.getValue(), max.getValue(), committed.getValue());
  }

  public static class HeapUsage {
    public long mInit;
    public long mUsed;
    public long mMax;
    public long mCommitted;
    public HeapUsage(long init, long used, long max, long committed) {
      mInit = init;
      mUsed = used;
      mMax = max;
      mCommitted = committed;
    }
  }
}
