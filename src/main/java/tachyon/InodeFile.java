package tachyon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.thrift.NetAddress;

public class InodeFile extends Inode {
  public static final long UNINITIAL_VALUE = -1;

  private long mLength;
  private boolean mPin = false;
  private boolean mCache = false;
  private String mCheckpointPath = "";

  private Map<Long, NetAddress> mLocations = new HashMap<Long, NetAddress>();

  public InodeFile(String name, int id, int parentId) {
    super(name, id, parentId, InodeType.File);
    mLength = UNINITIAL_VALUE;
  }

  public synchronized long getLength() {
    return mLength;
  }

  public synchronized void setLength(long length) {
    assert mLength == UNINITIAL_VALUE : "InodeFile length was set previously.";
    assert length < 0 : "InodeFile new length " + length + " is illegal.";
    mLength = length;
  }

  public synchronized boolean isReady() {
    return mLength != UNINITIAL_VALUE;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(", LENGTH:").append(mLength);
    sb.append(", CheckpointPath:").append(mCheckpointPath).append(")");
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
    if (ret.isEmpty() && hasCheckpointed()) {
      HdfsClient hdfsClient = new HdfsClient(mCheckpointPath);
      List<String> locs = hdfsClient.getFirstBlockLocations(mCheckpointPath);
      for (String loc: locs) {
        ret.add(new NetAddress(loc, -1));
      }
    }
    return ret;
  }

  public synchronized boolean isInMemory() {
    return mLocations.size() > 0;
  }

  public synchronized void setPin(boolean pin) {
    mPin = pin;
  }

  public synchronized boolean isPin() {
    return mPin;
  }

  public synchronized void setCache(boolean cache) {
    mCache = cache;
  }

  public synchronized boolean isCache() {
    return mCache;
  }

  public synchronized boolean hasCheckpointed() {
    return !mCheckpointPath.equals("");
  }
}