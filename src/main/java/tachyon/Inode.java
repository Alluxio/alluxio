package tachyon;

import tachyon.thrift.ClientFileInfo;

/**
 * <code>Inode</code> is an abstract class, with information shared by all types of Inodes.
 */
public abstract class Inode implements Comparable<Inode> {
  private final long CREATION_TIME_MS;
  protected final InodeType TYPE;

  private int mId;
  private String mName;
  private int mParentId;

  protected Inode(String name, int id, int parentId, InodeType type) {
    this(name, id, parentId, type, System.currentTimeMillis());
  }

  protected Inode(String name, int id, int parentId, InodeType type, long creationTimeMs) {
    TYPE = type;

    mId = id;
    mName = name;
    mParentId = parentId;

    CREATION_TIME_MS = creationTimeMs;
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
    return TYPE != InodeType.File;
  }

  public boolean isFile() {
    return TYPE == InodeType.File;
  }

  public long getCreationTimeMs() {
    return CREATION_TIME_MS;
  }

  public InodeType getInodeType() {
    return TYPE;
  }

  public synchronized int getId() {
    return mId;
  }

  public abstract ClientFileInfo generateClientFileInfo(String path);

  public synchronized void reverseId() {
    mId = - mId;
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

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("INode(");
    sb.append("ID:").append(mId).append(", NAME:").append(mName);
    sb.append(", PARENT_ID:").append(mParentId);
    sb.append(", CREATION_TIME_MS:").append(CREATION_TIME_MS).append(")");
    return sb.toString();
  }
}