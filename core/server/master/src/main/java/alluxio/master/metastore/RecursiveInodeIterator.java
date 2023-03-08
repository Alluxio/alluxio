package alluxio.master.metastore;

import alluxio.AlluxioURI;
import alluxio.master.file.meta.Inode;
import alluxio.resource.CloseableIterator;
import alluxio.resource.CloseableResource;

import java.util.Iterator;
import java.util.Stack;

public class RecursiveInodeIterator extends CloseableResource<Iterator<? extends Inode>>
    implements Iterator<Inode> {
  Stack<Iterator<? extends Inode>> mIteratorStack = new Stack<>();
  ReadOnlyInodeStore mInodeStore;
  boolean mRecursive;

  /**
   * Creates a {@link CloseableResource} wrapper around the given resource. This resource will
   * be returned by the {@link CloseableResource#get()} method.
   *
   * @param resource the resource to wrap
   */
  public RecursiveInodeIterator(
      ReadOnlyInodeStore inodeStore,
      Iterator<? extends Inode> initialIterator,
      boolean recursive
  ) {
    mInodeStore = inodeStore;
    mRecursive = recursive;
  }

  @Override
  public void closeResource() {

  }

  public AlluxioURI getCurrentURI() {
    // TODO get current URI
  }

  // TODO this implementation might be problematic
  // need to complete it later.
  @Override
  public void remove() {
    mIteratorStack.pop();
  }

  @Override
  public boolean hasNext() {
    while (!mIteratorStack.isEmpty() && !mIteratorStack.peek().hasNext()) {
      mIteratorStack.pop();
    }
    return !mIteratorStack.isEmpty();
  }

  @Override
  public Inode next() {
    Inode result = mIteratorStack.peek().next();
    if (mRecursive) {
      CloseableIterator<? extends Inode> nextLevelIterator =
          mInodeStore.getChildren(result.getId());
      if (nextLevelIterator.hasNext()) {
        mIteratorStack.push(nextLevelIterator);
      }
    }
    return result;
  }
}