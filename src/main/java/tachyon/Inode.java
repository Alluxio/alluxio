package tachyon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.thrift.NetAddress;

public abstract class Inode implements Comparable<Inode> {
  public static final long UNINITIAL_VALUE = -1;

  private final long CREATION_TIME_MS;
  private final boolean IS_FOLDER;

  private int mId;
  private String mName;
  private int mParentId;

  // TODO These two variables should be moved to InodeFile. 
  private boolean mPin = false;
  private boolean mCache = false;

  private boolean mHasCheckpointed = false;
  private String mCheckpointPath = "";

  private Map<Long, NetAddress> mLocations = new HashMap<Long, NetAddress>();

  protected Inode(String name, int id, int parentId, boolean isFolder) {
    mName = name;
    mParentId = parentId;

    mId = id;
    IS_FOLDER = isFolder;
    CREATION_TIME_MS = System.currentTimeMillis();
  }

  @Override
  public synchronized int compareTo(Inode o) {
    return mId - o.mId;
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (!(o instanceof Inode)) {
      return false;
    }
    return mId == ((Inode)o).mId;
  }

  @Override
  public synchronized int hashCode() {
    return mId;
  }

  public boolean isDirectory() {
    return IS_FOLDER;
  }

  public boolean isFile() {
    return !IS_FOLDER;
  }

  public long getCreationTimeMs() {
    return CREATION_TIME_MS;
  }

  public synchronized int getId() {
    return mId;
  }

  public synchronized void reverseId() {
    mId = -mId;
  }

  public synchronized String getName() {
    return mName;
  }

  public synchronized void setName(String name) {
    mName = name;
  }

  public synchronized int getParentId() {
    return mParentId;
  }

  public synchronized void setParentId(int parentId) {
    mParentId = parentId;
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

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("INode(");
    sb.append(mName).append(",").append(mId).append(",").append(mParentId).append(")");
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
}