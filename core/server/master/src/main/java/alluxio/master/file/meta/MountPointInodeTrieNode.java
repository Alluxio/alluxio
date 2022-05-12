package alluxio.master.file.meta;

import com.amazonaws.annotation.NotThreadSafe;
import com.google.protobuf.MapEntry;

import java.util.*;

@NotThreadSafe
public final class MountPointInodeTrieNode {
  private Map<InodeView, MountPointInodeTrieNode> mChildren = new HashMap<>();

  private boolean mIsMountPoint = false;

  public final int CHILDREN_INITIAL_CAPACITY = 5;
  /**
   * insert a list of inodes under current TrieNode
   * @param inodes inodes to be added to TrieNode
   * @param mIsMountPoint true if inodes describe a mountPath
   * @return
   */
  public MountPointInodeTrieNode insert(List<InodeView> inodes, boolean mIsMountPoint) {
    MountPointInodeTrieNode current = this;
    for(InodeView inode : inodes) {
      if (!current.mChildren.containsKey(inode)) {
        current.mChildren.put(inode, new MountPointInodeTrieNode());
      }
      current = current.mChildren.get(inode);
    }
    current.mIsMountPoint = mIsMountPoint;
    return current;
  }

  /**
   * find the lowest ancestor of inodes
   * @param inodes the target inodes
   * @param isMountPointFilter true if this ancestor must also be a mount point
   * @return
   */
  public MountPointInodeTrieNode lowestMatchedTrieNode(List<InodeView> inodes,
                                                      boolean isMountPointFilter) {
    MountPointInodeTrieNode ancestor = null;
    MountPointInodeTrieNode current = this;

    for (InodeView inode : inodes) {
      if(current.mIsMountPoint == isMountPointFilter) {
        ancestor = current;
      }
      if(current.mChildren.containsKey(inode)) {
        current = current.mChildren.get(inode);
      } else {
        break;
      }
    }
    return ancestor;
  }

  /**
   * acquire all the children TrieNodes and return
   * @param isMountPointFilter filter the children nodes' mountPoint prop
   * @return all the children TrieNodes
   */
  public List<InodeView> allChildren(boolean isMountPointFilter) {
    List<InodeView> childrenNodes = new ArrayList<>();
    if (isTerminal()) {
      return childrenNodes;
    }
    Queue<MountPointInodeTrieNode> queue = new LinkedList<>();
    queue.add(this);

    while(!queue.isEmpty()) {
      MountPointInodeTrieNode front = queue.poll();
      for(Map.Entry<InodeView, MountPointInodeTrieNode> entry : front.mChildren.entrySet()) {
        MountPointInodeTrieNode value = entry.getValue();
        queue.add(value);
        if(value.filterByMountPoint(isMountPointFilter)) {
          childrenNodes.add(entry.getKey());
        }
      }
    }
    return childrenNodes;
  }

  /**
   * Checks whether current TrieNode is the last one along the TrieNode path
   * @return
   */
  public boolean isTerminal() {
    return mChildren.size() == 0;
  }

  public boolean isMountPoint() {
    return mIsMountPoint;
  }

  public boolean filterByMountPoint(boolean isMountPointFilter) {
    return mIsMountPoint == isMountPointFilter;
  }
}
