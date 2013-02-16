package tachyon;

public class INodeFile extends INode {
  private static final long UNINITIAL_VALUE = -1;

  public long mLength;

  public INodeFile(String name, int parent) {
    super(name, parent);
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
