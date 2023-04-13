package alluxio.master.metastore;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeIterationResult;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.resource.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.StringJoiner;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * A recursive inode iterator that supports to skip children inodes during iteration.
 */
public class RecursiveInodeIterator implements SkippableInodeIterator {
  private static final Logger LOG = LoggerFactory.getLogger(RecursiveInodeIterator.class);

  Stack<Pair<CloseableIterator<? extends Inode>, LockedInodePath>> mIteratorStack = new Stack<>();
  ReadOnlyInodeStore mInodeStore;
  boolean mHasNextCalled = false;
  boolean mHasNext;
  List<String> mNameComponents = new ArrayList<>();
  List<String> mStartAfterPathComponents;
  LockedInodePath mLastLockedPath = null;
  Inode mFirst;
  LockedInodePath mRootPath;

  /**
   * Constructs an instance.
   *
   * @param inodeStore the inode store
   * @param inode    the root inode
   * @param readOption the read option
   * @param lockedPath the locked path to the root inode
   */
  public RecursiveInodeIterator(
      ReadOnlyInodeStore inodeStore,
      Inode inode,
      ReadOption readOption,
      LockedInodePath lockedPath
  ) {
    mFirst = inode;
    mRootPath = lockedPath;
    String startFrom = readOption.getStartFrom();
    if (startFrom == null) {
      mStartAfterPathComponents = Collections.emptyList();
    } else {
      try {
        startFrom = readOption.getStartFrom().startsWith(AlluxioURI.SEPARATOR)
            ? readOption.getStartFrom().substring(1) : readOption.getStartFrom();
        mStartAfterPathComponents = Arrays.asList(startFrom
            .split(AlluxioURI.SEPARATOR));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    ReadOption firstReadOption;
    if (mStartAfterPathComponents.size() > 0) {
      firstReadOption =
          ReadOption.newBuilder().setReadFrom(mStartAfterPathComponents.get(0)).build();
    } else {
      firstReadOption = ReadOption.defaults();
    }
    mIteratorStack.push(new Pair<>(inodeStore.getChildren(
        inode.getId(), firstReadOption), lockedPath));
    mInodeStore = inodeStore;
  }

  // The locked inode path will become stale after skipChildrenOfTheCurrent() is called.
  @Override
  public void skipChildrenOfTheCurrent() {
    if (mHasNextCalled) {
      throw new IllegalStateException("Cannot call hasNext");
    }
    popStack();
    if (mNameComponents.size() > 0) {
      mNameComponents.remove(mNameComponents.size() - 1);
    }
  }

  private void popStack() {
    Pair<CloseableIterator<? extends Inode>, LockedInodePath> item = mIteratorStack.pop();
    item.getFirst().close();
    if (!mIteratorStack.isEmpty()) {
      item.getSecond().close();
    }
  }

  @Override
  public boolean hasNext() {
    if (mFirst != null) {
      return true;
    }
    if (mHasNextCalled) {
      return mHasNext;
    }
    while (!mIteratorStack.isEmpty() && !tryOnIterator(
        mIteratorStack.peek().getFirst(), CloseableIterator::hasNext
    )) {
      popStack();
      // When the iteration finishes, the size of mPathComponents is 0
      if (mNameComponents.size() > 0) {
        mNameComponents.remove(mNameComponents.size() - 1);
      }
    }
    mHasNextCalled = true;
    mHasNext = !mIteratorStack.isEmpty();
    return mHasNext;
  }

  @Override
  public InodeIterationResult next() {
    if (!hasNext()) {
      throw new InternalRuntimeException("Called next on a completed iterator");
    }
    if (mFirst != null) {
      Inode ret = mFirst;
      mFirst = null;
      return new InodeIterationResult(ret, ret.getName(), mRootPath);
    }
    Pair<CloseableIterator<? extends Inode>, LockedInodePath> top = mIteratorStack.peek();
    try {
      top.getSecond().traverse();
    } catch (InvalidPathException e) {
      // should not reach here as the path is valid
      throw new InternalRuntimeException(e);
    }
    if (mLastLockedPath != null) {
      mLastLockedPath.close();
      mLastLockedPath = null;
    } else {
      if (top.getSecond().getLockPattern() != InodeTree.LockPattern.READ) {
        // after the parent has been returned, we can downgrade it to a read lock
        top.getSecond().downgradeToRead();
      }
    }
    Inode current = tryOnIterator(top.getFirst(), CloseableIterator::next);
    LockedInodePath lockedPath;
    try {
      lockedPath = top.getSecond().lockChild(current, InodeTree.LockPattern.WRITE_EDGE, false);
    } catch (InvalidPathException e) {
      // should not reach here as the path is valid
      throw new InternalRuntimeException(e);
    }
    StringJoiner joiner = new StringJoiner("/");
    for (String nxt : mNameComponents) {
      joiner.add(nxt);
    }
    joiner.add(current.getName());
    String name = joiner.toString();
    if (current.isDirectory()) {
      ReadOption readOption = ReadOption.newBuilder()
          .setReadFrom(populateStartAfter(current.getName())).build();
      CloseableIterator<? extends Inode> nextLevelIterator =
          mInodeStore.getChildren(current.getId(), readOption);
      mIteratorStack.push(new Pair<>(nextLevelIterator, lockedPath));
      mNameComponents.add(current.getName());
    } else {
      mLastLockedPath = lockedPath;
    }
    mHasNextCalled = false;
    return new InodeIterationResult(current, name, lockedPath);
  }

  // TODO add comments
  private @Nullable String populateStartAfter(String currentInodeName) {
    if (mNameComponents.size() + 1 >= mStartAfterPathComponents.size()) {
      return null;
    }
    for (int i = 0; i < mNameComponents.size(); ++i) {
      if (!mNameComponents.get(i).equals(mStartAfterPathComponents.get(i))) {
        return null;
      }
    }
    if (!currentInodeName.equals(mStartAfterPathComponents.get(mNameComponents.size()))) {
      return null;
    }
    return mStartAfterPathComponents.get(mNameComponents.size() + 1);
  }

  private <T> T tryOnIterator(CloseableIterator<? extends Inode> iterator,
                              Function<CloseableIterator<? extends Inode>, T> supplier) {
    try {
      return supplier.apply(iterator);
    } catch (Exception e) {
      iterator.close();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (mLastLockedPath != null) {
      mLastLockedPath.close();
      mLastLockedPath = null;
    }
    while (!mIteratorStack.isEmpty()) {
      popStack();
    }
  }
}
