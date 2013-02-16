package tachyon;

public abstract class INode implements Comparable<INode> {
  protected long mCreationTimeMs;
  protected int mId;
  protected String mName;
  protected int mParent;

  protected INode(String name, int parent) {
    mName = name;
    mParent = parent;
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

  public abstract boolean isDirectory();

  public abstract boolean isFile();

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
}