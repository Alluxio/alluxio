package tachyon;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The structure to store a worker's information.
 * @author Haoyuan
 */
public class WorkerInfo {
  public final InetSocketAddress ADDRESS;
  public final long TOTAL_BYTES;
  private final long START_TIME_MS;

  private long mId;
  private long mUsedBytes;
  private long mLastUpdatedTimeMs;
  private Set<Long> mPartitions; 

  public WorkerInfo(long id, InetSocketAddress address, long totalBytes) {
    mId = id;
    ADDRESS = address;
    TOTAL_BYTES = totalBytes;
    START_TIME_MS = System.currentTimeMillis();

    mUsedBytes = 0;
    mPartitions = new HashSet<Long>();
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  public synchronized boolean containPartition(long partition) {
    return mPartitions.contains(partition);
  }

  public synchronized long getAvailableBytes() {
    return TOTAL_BYTES - mUsedBytes;
  }

  public synchronized long getId() {
    return mId;
  }

  public synchronized long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  public synchronized Set<Long> getPartitions() {
    return new HashSet<Long>(mPartitions);
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

  public synchronized void removePartition(long bigId) {
    mPartitions.remove(bigId);
  }

  public synchronized String toHtml() {
    long timeMs = System.currentTimeMillis() - START_TIME_MS;
    StringBuilder sb = new StringBuilder("has been running @ " + ADDRESS + 
        " for " + CommonUtils.convertMillis(timeMs));
    sb.append("<br \\>");
    sb.append(" ID ").append(mId).append(" ; ");
    sb.append("LastUpdateTime ").append(CommonUtils.convertMillisToDate(mLastUpdatedTimeMs)).append("<br \\>");
    sb.append("Capacity (Byte): ").append(TOTAL_BYTES);
    sb.append("; UsedSpace: ").append(mUsedBytes);
    sb.append("; AvailableSpace: ").append(TOTAL_BYTES - mUsedBytes).append("<br \\>");

    sb.append("Partitions: [ ");
    List<Long> partitions = new ArrayList<Long>(mPartitions);
    Collections.sort(partitions);
    for (Long partition : partitions) {
      sb.append("(").append(CommonUtils.computeDatasetIdFromBigId(partition));
      sb.append(":").append(CommonUtils.computePartitionIdFromBigId(partition)).append(") ");
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("WorkerInfo(");
    sb.append(" ID: ").append(mId);
    sb.append(", ADDRESS: ").append(ADDRESS);
    sb.append(", TOTAL_BYTES: ").append(TOTAL_BYTES);
    sb.append(", mUsedBytes: ").append(mUsedBytes);
    sb.append(", mAvailableBytes: ").append(TOTAL_BYTES - mUsedBytes);
    sb.append(", mLastUpdatedTimeMs: ").append(mLastUpdatedTimeMs);
    sb.append(", mPartitions: [ ");
    for (Long partition : mPartitions) {
      sb.append(partition).append("(").append(CommonUtils.computeDatasetIdFromBigId(partition))
      .append(":").append(CommonUtils.computePartitionIdFromBigId(partition)).append(") ; ");
    }
    sb.append("] )");
    return sb.toString();
  }

  public synchronized void updateId(long id) {
    mId = id;
  }

  public synchronized void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  public synchronized void updatePartition(boolean add, long partition) {
    if (add) {
      mPartitions.add(partition);
    } else {
      mPartitions.remove(partition);
    }
  }

  public synchronized void updatePartitions(boolean add, Collection<Long> partitions) {
    if (add) {
      mPartitions.addAll(partitions);
    } else {
      mPartitions.removeAll(partitions);
    }
  }

  public synchronized void updateUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }
}