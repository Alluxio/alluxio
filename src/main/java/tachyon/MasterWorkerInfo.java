package tachyon;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.NetAddress;

/**
 * The structure to store a worker's information in master node.
 */
public class MasterWorkerInfo {
  public final InetSocketAddress ADDRESS;
  private final long CAPACITY_BYTES;
  private final long START_TIME_MS;

  private long mId;
  private long mUsedBytes;
  private long mLastUpdatedTimeMs;
  private Set<Integer> mFiles;
  private Set<Integer> mToRemoveFiles;

  public MasterWorkerInfo(long id, InetSocketAddress address, long capacityBytes) {
    mId = id;
    ADDRESS = address;
    CAPACITY_BYTES = capacityBytes;
    START_TIME_MS = System.currentTimeMillis();

    mUsedBytes = 0;
    mFiles = new HashSet<Integer>();
    mToRemoveFiles = new HashSet<Integer>();
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  public InetSocketAddress getAddress() {
    return ADDRESS;
  }

  public synchronized long getAvailableBytes() {
    return CAPACITY_BYTES - mUsedBytes;
  }

  public long getCapacityBytes() {
    return CAPACITY_BYTES;
  }

  public synchronized long getId() {
    return mId;
  }

  public synchronized long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  public synchronized Set<Integer> getFiles() {
    return new HashSet<Integer>(mFiles);
  }

  public synchronized List<Integer> getToRemovedFiles() {
    return new ArrayList<Integer>(mToRemoveFiles);
  }

  public synchronized long getUsedBytes() {
    return mUsedBytes;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("MasterWorkerInfo(");
    sb.append(" ID: ").append(mId);
    sb.append(", ADDRESS: ").append(ADDRESS);
    sb.append(", TOTAL_BYTES: ").append(CAPACITY_BYTES);
    sb.append(", mUsedBytes: ").append(mUsedBytes);
    sb.append(", mAvailableBytes: ").append(CAPACITY_BYTES - mUsedBytes);
    sb.append(", mLastUpdatedTimeMs: ").append(mLastUpdatedTimeMs);
    sb.append(", mFiles: [ ");
    for (int file : mFiles) {
      sb.append(file).append(", ");
    }
    sb.append("] )");
    return sb.toString();
  }

  public synchronized void updateFile(boolean add, int fileId) {
    if (add) {
      mFiles.add(fileId);
    } else {
      mFiles.remove(fileId);
    }
  }

  public synchronized void updateFiles(boolean add, Collection<Integer> fileIds) {
    if (add) {
      mFiles.addAll(fileIds);
    } else {
      mFiles.removeAll(fileIds);
    }
  }

  public synchronized void updateToRemovedFile(boolean add, int fileId) {
    if (add) {
      if (mFiles.contains(fileId)) {
        mToRemoveFiles.add(fileId);
      }
    } else {
      mToRemoveFiles.remove(fileId);
    }
  }
  
  public synchronized void updateToRemovedFiles(boolean add, Collection<Integer> fileIds) {
    for (int fileId: fileIds) {
      updateToRemovedFile(add, fileId);
    }
  }

  public synchronized void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  public synchronized void updateUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }

  public synchronized ClientWorkerInfo generateClientWorkerInfo() {
    ClientWorkerInfo ret = new ClientWorkerInfo();
    ret.id = mId;
    ret.address = new NetAddress(ADDRESS.getHostName(), ADDRESS.getPort());
    ret.lastContactSec = (int) ((CommonUtils.getCurrentMs() - mLastUpdatedTimeMs) / 1000);
    ret.state = "In Service";
    ret.capacityBytes = CAPACITY_BYTES;
    ret.usedBytes = mUsedBytes;
    ret.starttimeMs = START_TIME_MS;
    return ret;
  }
}