package tachyon;

public class INodeFile extends INode {
  private static final long serialVersionUID = -1234480668664604254L;

  private boolean mIsReady;
  private long mLength;

  public INodeFile(String name, int id, int parentId) {
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
}
