package tachyon;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import tachyon.thrift.NetAddress;

public abstract class INode implements Comparable<INode>, Serializable {
  private static final long serialVersionUID = 2712957988375300023L;

  public static final long UNINITIAL_VALUE = -1;

  protected long mCreationTimeMs;
  protected int mId;
  protected String mName;
  protected int mParentId;
  protected boolean mIsFolder;

  protected boolean mPin = false;
  protected boolean mCache = false;
  
  protected boolean mHasCheckpointed = false;
  protected String mCheckpointPath = "";

  public Map<Long, NetAddress> mLocations = new HashMap<Long, NetAddress>();

  protected INode(String name, int id, int parentId, boolean isFolder) {
    mName = name;
    mId = id;
    mParentId = parentId;
    mCreationTimeMs = System.currentTimeMillis();
  }

  @Override
  public int compareTo(INode o) {
    return mId - o.mId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof INode)) {
      return false;
    }
    return mId == ((INode)o).mId;
  }

  @Override
  public int hashCode() {
    return mId;
  }

  public abstract long getLength();

  public boolean isDirectory() {
    return mIsFolder;
  }

  public boolean isFile() {
    return !mIsFolder;
  }

  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  public int getId() {
    return mId;
  }

  public String getName() {
    return mName;
  }

  public void setName(String name) {
    mName = name;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("INode(");
    sb.append(mName).append(",").append(mId).append(")");
    return sb.toString();
  }
}