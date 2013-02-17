package tachyon;

public class INodeFile extends INode {
  private static final long serialVersionUID = -1234480668664604254L;

  private long mLength;

  public INodeFile(String name, int id, int parentId) {
    super(name, id, parentId, false);
    mLength = UNINITIAL_VALUE;
  }

  @Override
  public long getLength() {
    return mLength;
  }

  public void setLength(long length) {
    assert mLength == UNINITIAL_VALUE : "INodeFile length was set previously.";
    mLength = length;
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  @Override
  public boolean isFile() {
    return true;
  }
}
