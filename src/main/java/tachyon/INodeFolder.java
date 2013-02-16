package tachyon;

import java.util.ArrayList;
import java.util.List;

public class INodeFolder extends INode {
  private List<INode> mChildren;

  public INodeFolder(String name, int parent) {
    super(name, parent);
    mChildren = new ArrayList<INode>();
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public boolean isDirectory() {
    return true;
  }

  @Override
  public boolean isFile() {
    return false;
  }

  public synchronized void addChild(INode childId) {
    mChildren.add(childId);
  }

  public synchronized void addChildren(INode [] childrenIds) {
    for (int k = 0; k < childrenIds.length; k ++) {
      addChild(childrenIds[k]);
    }
  }

  public synchronized INode getChild(String name) {
    for (int k = 0; k < mChildren.size(); k ++) {
      if (mChildren.get(k).getName().equals(name)) {
        return mChildren.get(k);
      }
    }
    return null;
  }

  public synchronized boolean removeChild(String name) {
    for (int k = 0; k < mChildren.size(); k ++) {
      if (mChildren.get(k).getName().equals(name)) {
        mChildren.remove(k);
        return true;
      }
    }
    return false;
  }
}
