package tachyon;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InodeFolder extends Inode {
  private static final long serialVersionUID = -1195474593218285949L;

  private Set<Integer> mChildren;
  
  private final boolean IS_RAW_TABLE;

  public InodeFolder(String name, int id, int parentId) {
    this(name, id, parentId, false);
  }
  
  public InodeFolder(String name, int id, int parentId, boolean isRawTable) {
    super(name, id, parentId, true);
    mChildren = new HashSet<Integer>();
    IS_RAW_TABLE = isRawTable;
  }

  public synchronized void addChild(int childId) {
    mChildren.add(childId);
  }

  public synchronized void addChildren(int [] childrenIds) {
    for (int k = 0; k < childrenIds.length; k ++) {
      addChild(childrenIds[k]);
    }
  }

  public synchronized Inode getChild(String name, Map<Integer, Inode> allInodes) {
    Inode tInode = null;
    for (int childId : mChildren) {
      tInode = allInodes.get(childId);
      if (tInode != null && tInode.getName().equals(name)) {
        return tInode;
      }
    }
    return null;
  }

  public synchronized List<Integer> getChildrenIds() {
    List<Integer> ret = new ArrayList<Integer>(mChildren.size());
    ret.addAll(mChildren);
    return ret;
  }

  public synchronized void removeChild(int id) {
    mChildren.remove(id);
  }

  public synchronized boolean removeChild(String name, Map<Integer, Inode> allInodes) {
    Inode tInode = null;
    for (int childId : mChildren) {
      tInode = allInodes.get(childId);
      if (tInode != null && tInode.getName().equals(name)) {
        mChildren.remove(childId);
        return true;
      }
    }
    return false;
  }
  
  public boolean isRawTable() {
    return IS_RAW_TABLE;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFolder(");
    sb.append(super.toString()).append(",").append(mChildren).append(")");
    return sb.toString();
  }
}