package tachyon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.thrift.NetAddress;

public class InodeFile extends Inode {
  public static final long UNINITIAL_VALUE = -1;

  private boolean mIsReady;
  private long mLength;

  private boolean mPin = false;
  private boolean mCache = false;

  private boolean mHasCheckpointed = false;
  private String mCheckpointPath = "";

  private Map<Long, NetAddress> mLocations = new HashMap<Long, NetAddress>();

  public InodeFile(String name, int id, int parentId) {
    super(name, id, parentId, false);
    mLength = UNINITIAL_VALUE;
    mIsReady = false;
  }

  public synchronized long getLength() {
    return mLength;
  }

  public synchronized void setLength(long length) {
    assert mLength == UNINITIAL_VALUE : "INodeFile length was set previously.";
    mLength = length;
    mIsReady = true;
  }

  public synchronized boolean isReady() {
    return mIsReady;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(",").append(mLength).append(",");
    sb.append(mHasCheckpointed).append(",").append(mCheckpointPath).append(")");
    return sb.toString();
  }

  public synchronized void setCheckpointPath(String checkpointPath) {
    mCheckpointPath = checkpointPath;
  }

  public synchronized String getCheckpointPath() {
    return mCheckpointPath;
  }

  public synchronized void addLocation(long workerId, NetAddress workerAddress) {
    mLocations.put(workerId, workerAddress);
  }

  public synchronized void removeLocation(long workerId) {
    mLocations.remove(workerId);
  }

  public synchronized List<NetAddress> getLocations() {
    List<NetAddress> ret = new ArrayList<NetAddress>(mLocations.size());
    ret.addAll(mLocations.values());
    return ret;
  }

  public synchronized boolean isInMemory() {
    return mLocations.size() > 0;
  }

  public void setPin(boolean pin) {
    mPin = pin;
  }

  public synchronized boolean isPin() {
    return mPin;
  }

  public synchronized boolean isCache() {
    return mCache;
  }

  public synchronized void setHasCheckpointed(boolean hasCheckpointed) {
    mHasCheckpointed = hasCheckpointed;
  }

  public synchronized boolean hasCheckpointed() {
    return mHasCheckpointed;
  }
}
