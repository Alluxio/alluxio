package alluxio.master.metastore;

import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeIterationResult;
import alluxio.resource.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * A recursive inode iterator that supports to skip children inodes during iteration.
 */
public class RecursiveInodeIterator implements SkippableInodeIterator {
  private static final Logger LOG = LoggerFactory.getLogger(RecursiveInodeIterator.class);

  Stack<CloseableIterator<? extends Inode>> mIteratorStack = new Stack<>();
  ReadOnlyInodeStore mInodeStore;
  boolean mHasNextCalled = false;
  List<String> mNameComponents = new ArrayList<>();
  List<String> mStartAfterPathComponents;

  /**
   * Constructs an instance.
   *
   * @param inodeStore the inode store
   * @param inodeId    the root inode id
   * @param readOption the read option
   */
  public RecursiveInodeIterator(
      ReadOnlyInodeStore inodeStore,
      long inodeId,
      ReadOption readOption
  ) {
    String startFrom = readOption.getStartFrom();
    if (startFrom == null) {
      mStartAfterPathComponents = Collections.emptyList();
    } else {
      try {
        mStartAfterPathComponents = Arrays.asList(readOption.getStartFrom()
            .split("/"));
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
    mIteratorStack.push(inodeStore.getChildren(inodeId, firstReadOption));
    mInodeStore = inodeStore;
  }

  @Override
  public void skipChildrenOfTheCurrent() {
    if (mHasNextCalled) {
      throw new IllegalStateException("Cannot call hasNext");
    }
    mIteratorStack.pop().close();
    mNameComponents.remove(mNameComponents.size() - 1);
  }

  @Override
  public boolean hasNext() {
    while (!mIteratorStack.isEmpty() && !tryOnIterator(
        mIteratorStack.peek(), CloseableIterator::hasNext
    )) {
      mIteratorStack.pop().close();
      // When the iteration finishes, the size of mPathComponents is 0
      if (mNameComponents.size() > 0) {
        mNameComponents.remove(mNameComponents.size() - 1);
      }
    }
    mHasNextCalled = true;
    return !mIteratorStack.isEmpty();
  }

  @Override
  public InodeIterationResult next() {
    Inode current = tryOnIterator(mIteratorStack.peek(), CloseableIterator::next);
    ReadOption readOption = ReadOption.newBuilder()
        .setReadFrom(populateStartAfter(current.getName())).build();
    CloseableIterator<? extends Inode> nextLevelIterator =
        mInodeStore.getChildren(current.getId(), readOption);
    mIteratorStack.push(nextLevelIterator);
    mNameComponents.add(current.getName());
    mHasNextCalled = false;
    return new InodeIterationResult(current, String.join("/", mNameComponents));
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
    while (!mIteratorStack.isEmpty()) {
      CloseableIterator<? extends Inode> iterator = mIteratorStack.pop();
      try {
        iterator.close();
      } catch (Exception e) {
        LOG.error("Closing resource " + iterator + "failed");
      }
    }
  }
}
