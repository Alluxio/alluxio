package tachyon;

public class InodeFile extends Inode {
  private boolean mIsReady;
  private long mLength;

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
    sb.append(super.toString()).append(",").append(mLength).append(")");
    return sb.toString();
  }
}
