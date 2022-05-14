package alluxio.master.file.meta;

import alluxio.collections.Pair;
import com.amazonaws.annotation.NotThreadSafe;

import java.util.*;

@NotThreadSafe
public final class MountPointInodeTrieNode<T> {
  private Map<T, MountPointInodeTrieNode<T>> mChildren = new HashMap<>();

  // mLists is valid only when mIsTerminal is true
  private List<T> mList = null;

  private boolean mIsMountPoint = false;

  private boolean mIsTerminal = false;

  /**
   * insert a list of inodes under current TrieNode
   * @param inodes inodes to be added to TrieNode
   * @param mIsMountPoint true if inodes describe a mountPath
   * @return the last created TrieNode based on inodes
   */
  public MountPointInodeTrieNode<T> insert(List<T> inodes, boolean mIsMountPoint) {
    MountPointInodeTrieNode<T> current = this;
    for(T inode : inodes) {
      if (!current.mChildren.containsKey(inode)) {
        current.mChildren.put(inode, new MountPointInodeTrieNode<T>());
      }
      current = current.mChildren.get(inode);
    }
    current.mIsMountPoint = mIsMountPoint;
    current.mIsTerminal = true;
    if(mIsTerminal) {
      current.mList = new ArrayList<>(mList);
      current.mList.addAll(inodes);
    } else {
      current.mList = new ArrayList<>(inodes);
    }
    return current;
  }

  /**
   * find the lowest matched TrieNode of given inodes
   * @param inodes the target inodes
   * @param predicate true if this matched TrieNode must also be a mount point
   * @param isCompleteMatch true if the TrieNode must completely match the given inodes
   * @return null if there is no valid TrieNode, else return the lowest matched TrieNode
   */
  public MountPointInodeTrieNode<T> lowestMatchedTrieNode(List<T> inodes,
                                                       java.util.function.Function<MountPointInodeTrieNode, Boolean> predicate,
                                                       boolean isCompleteMatch) {
    MountPointInodeTrieNode<T> matchedPos = null;
    MountPointInodeTrieNode<T> current = this;

    if(inodes.isEmpty() && this.mIsTerminal && predicate.apply(this)) {
      return this;
    }
    for (int i = 0; i<inodes.size(); i++) {
      T inode = inodes.get(i);
      if(!current.mChildren.containsKey(inode)) {
        if(isCompleteMatch) {
          return null;
        }
        break;
      }
      current = current.mChildren.get(inode);
      if(current.mIsTerminal && predicate.apply(current) && (!isCompleteMatch || i == inodes.size() - 1)) {
        matchedPos = current;
      }
    }
    return matchedPos;
  }

  /**
   * find the child among current TrieNode's direct children that have key as its identifier
   * @param key the key of searched child
   * @return not null if the valid child exists, else return null
   */
  public MountPointInodeTrieNode<T> child(T key, java.util.function.Function<MountPointInodeTrieNode<T>, Boolean> predicate) {
    if(isLastTrieNode()) {
      return null;
    }
    MountPointInodeTrieNode<T> node = mChildren.get(key);
    if(node != null && predicate.apply(node)) {
      return node;
    }
    return null;
  }

  /**
   * acquire the direct children's TrieNodes of current TrieNode
   * @return the direct children's TrieNodes of current Node
   */
  public Collection<MountPointInodeTrieNode<T>> childrenTrieNodes(java.util.function.Function<MountPointInodeTrieNode<T>, Boolean> predicate) {
    Collection<MountPointInodeTrieNode<T>> children = new HashSet<>();
    for(MountPointInodeTrieNode<T> node : mChildren.values()) {
      if(predicate.apply(node)) {
        children.add(node);
      }
    }
    return children;
  }

  /**
   * acquire the direct children's keys of current TrieNode
   * @return the direct children's keys of current TrieNode
   */
  public Collection<T> childrenKeys(java.util.function.Function<MountPointInodeTrieNode<T>, Boolean> predicate) {
    Collection<T> children = new HashSet<>();
    for(T key : mChildren.keySet()) {
      if(predicate.apply(mChildren.get(key))) {
        children.add(key);
      }
    }
    return children;
  }

  /**
   * acquire all the children TrieNodes
   * @param predicate filter the children nodes
   * @return all the children TrieNodes
   */
  public List<T> allChildren(java.util.function.Function<MountPointInodeTrieNode<T>, Boolean> predicate) {
    List<T> childrenNodes = new ArrayList<>();
    if (isLastTrieNode()) {
      return childrenNodes;
    }
    Queue<MountPointInodeTrieNode<T>> queue = new LinkedList<>();
    queue.add(this);

    while(!queue.isEmpty()) {
      MountPointInodeTrieNode<T> front = queue.poll();
      for(Map.Entry<T, MountPointInodeTrieNode<T>> entry : front.mChildren.entrySet()) {
        MountPointInodeTrieNode<T> value = entry.getValue();
        queue.add(value);
        if(value.mIsTerminal && predicate.apply(value)) {
          childrenNodes.add(entry.getKey());
        }
      }
    }
    return childrenNodes;
  }

  public MountPointInodeTrieNode<T> remove(List<T> inodes,
                                           java.util.function.Function<MountPointInodeTrieNode<T>, Boolean> predicate) {
    Stack<Pair<MountPointInodeTrieNode<T>, T>> parents = new Stack<>();
    MountPointInodeTrieNode<T> current = this;
    for(T inode : inodes) {
      if(!current.mChildren.containsKey(inode)) {
        return null;
      }
      parents.push(new Pair<>(current, inode));
      current = current.mChildren.get(inode);
    }
    if(!current.mIsTerminal || !predicate.apply(current)) {
      return null;
    }
    MountPointInodeTrieNode<T> nodeToRemove = current;
    current.mIsTerminal = false;
    while(current.isLastTrieNode() && !current.mIsTerminal && !parents.empty()) {
      Pair<MountPointInodeTrieNode<T>, T> parent = parents.pop();
      current = parent.getFirst();
      current.mChildren.remove(parent.getSecond());
    }
    return nodeToRemove;
  }

  /**
   * return a copy of current List, if it is not a terminal path, then return null
   * @return the copy of current list
   */
  public List<T> list() {
    if(!isTerminal()) {
      return null;
    }
    return new ArrayList<>(mList);
  }

  /**
   * Checks whether current TrieNode is the last one along the TrieNode path
   * @return true if current TrieNode is the last one along the path
   */
  public boolean isLastTrieNode() {
    return mChildren.isEmpty();
  }

  /**
   * Checks whether current TrieNode is a mountPoint
   * @return true if current TrieNode is a mountPoint
   */
  public boolean isMountPoint() {
    return mIsMountPoint;
  }

  public boolean isTerminal() {
    return mIsTerminal;
  }
}
