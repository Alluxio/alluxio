package tachyon.worker;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;

import static tachyon.metrics.Metrics.metrics;
import static tachyon.metrics.Metrics.name;

/**
 * The worker space counter, in charge of counting and granting spaces in a worker daemon.
 */
public class WorkerSpaceCounter {
  private final long mCapacityBytes;
  private long mUsedBytes;

  {
    metrics().register(name("capacity", "max"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getCapacityBytes();
      }
    });
    metrics().register(name("capacity", "used"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getUsedBytes();
      }
    });
    metrics().register(name("capacity", "free"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getAvailableBytes();
      }
    });
  }
  private final Histogram RETURN_FAILURE_HISTO = metrics().histogram(name("request", "failure", "size"));
  private final Histogram RETURN_SUCCESS_HISTO = metrics().histogram(name("request", "success", "size"));
  private final Histogram RETURN_HISTO = metrics().histogram(name("return", "size"));

  /**
   * @param capacityBytes
   *          The maximum memory space the TachyonWorker can use, in bytes
   */
  public WorkerSpaceCounter(long capacityBytes) {
    mCapacityBytes = capacityBytes;
    mUsedBytes = 0;
  }

  /**
   * @return The available space size, in bytes
   */
  public synchronized long getAvailableBytes() {
    return mCapacityBytes - mUsedBytes;
  }

  /**
   * @return The maximum memory space the TachyonWorker can use, in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return The bytes that have been used
   */
  public synchronized long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * Request space
   * 
   * @param requestSpaceBytes
   *          The requested space size, in bytes
   * @return
   */
  public synchronized boolean requestSpaceBytes(long requestSpaceBytes) {
    if (getAvailableBytes() < requestSpaceBytes) {
      RETURN_FAILURE_HISTO.update(requestSpaceBytes);
      return false;
    }

    mUsedBytes += requestSpaceBytes;
    RETURN_SUCCESS_HISTO.update(requestSpaceBytes);
    return true;
  }

  /**
   * Return used space size
   * 
   * @param returnUsedBytes
   *          The returned space size, in bytes
   */
  public synchronized void returnUsedBytes(long returnUsedBytes) {
    mUsedBytes -= returnUsedBytes;
    RETURN_HISTO.update(returnUsedBytes);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("WorkerSpaceCounter(");
    sb.append(" TOTAL_BYTES: ").append(mCapacityBytes);
    sb.append(", mUsedBytes: ").append(mUsedBytes);
    sb.append(", mAvailableBytes: ").append(mCapacityBytes - mUsedBytes);
    sb.append(" )");
    return sb.toString();
  }

  /**
   * Update the used bytes
   * 
   * @param usedBytes
   *          The new used bytes
   */
  public synchronized void updateUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }
}