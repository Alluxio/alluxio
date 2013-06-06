package tachyon;

/**
 * The worker space counter, in charge of counting and granting spaces in a worker daemon.
 */
public class WorkerSpaceCounter {
  private final long CAPACITY_BYTES;
  private long mUsedBytes;

  public WorkerSpaceCounter(long capacityBytes) {
    CAPACITY_BYTES = capacityBytes;
    mUsedBytes = 0;
  }

  public synchronized long getAvailableBytes() {
    return CAPACITY_BYTES - mUsedBytes;
  }

  public long getCapacityBytes() {
    return CAPACITY_BYTES;
  }

  public synchronized long getUsedBytes() {
    return mUsedBytes;
  }

  public synchronized boolean requestSpaceBytes(long requestSpaceBytes) {
    if (getAvailableBytes() < requestSpaceBytes) {
      return false;
    }

    mUsedBytes += requestSpaceBytes;
    return true;
  }

  public synchronized void returnUsedBytes(long returnUsedBytes) {
    mUsedBytes -= returnUsedBytes;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("WorkerSpaceCounter(");
    sb.append(" TOTAL_BYTES: ").append(CAPACITY_BYTES);
    sb.append(", mUsedBytes: ").append(mUsedBytes);
    sb.append(", mAvailableBytes: ").append(CAPACITY_BYTES - mUsedBytes);
    sb.append(" )");
    return sb.toString();
  }

  public synchronized void updateUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }
}