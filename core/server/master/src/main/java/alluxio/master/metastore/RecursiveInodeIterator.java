package alluxio.master.metastore;

import alluxio.AlluxioURI;
import alluxio.master.file.meta.Inode;
import alluxio.resource.CloseableIterator;
import alluxio.resource.CloseableResource;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;
import javax.annotation.Nullable;

public class RecursiveInodeIterator extends CloseableResource<Iterator<? extends Inode>>
    implements Iterator<Inode> {
  private static final Logger LOG = LoggerFactory.getLogger(RecursiveInodeIterator.class);

  Stack<CloseableIterator<? extends Inode>> mIteratorStack = new Stack<>();
  ReadOnlyInodeStore mInodeStore;
  boolean mRecursive;
  Inode mCurrent;
  boolean mHasNextCalled = false;
  List<String> mPathComponents = new ArrayList<>();
  List<String> mStartAfterPathComponents;

  /**
   * Creates a {@link CloseableResource} wrapper around the given resource. This resource will
   * be returned by the {@link CloseableResource#get()} method.
   *
   * @param resource the resource to wrap
   */
  public RecursiveInodeIterator(
      ReadOnlyInodeStore inodeStore,
      long inodeId,
      ReadOption readOption,
      boolean recursive
  ) {
    super(null);
    String startFrom = readOption.getStartFrom();
    if (startFrom == null) {
      mStartAfterPathComponents = Collections.emptyList();
    } else {
      try {
        mStartAfterPathComponents = Arrays.asList(readOption.getStartFrom().split("/")); //Arrays.asList(PathUtils.getPathComponents(readOption.getStartFrom()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    ReadOption firstReadOption;
    if (mStartAfterPathComponents.size() > 0 ) {
      firstReadOption = ReadOption.newBuilder().setReadFrom(mStartAfterPathComponents.get(0)).build();
    } else {
      firstReadOption = ReadOption.defaults();
    }
    mIteratorStack.push(inodeStore.getChildren(inodeId, firstReadOption));
    mInodeStore = inodeStore;
    mRecursive = recursive;
    // Add root
//    mPathComponents.add("");
  }

  @Override
  public void closeResource() {
    while (!mIteratorStack.isEmpty()) {
      CloseableIterator<? extends Inode> iterator = mIteratorStack.pop();
      try {
        iterator.close();
      } catch (Exception e) {
        LOG.error("Closing resource " + iterator + "failed");
      }
    }
  }

  public Inode current() {
    return mCurrent;
  }

  public List<String> getCurrentURI() {
    if (mHasNextCalled) {
      throw new IllegalStateException("Cannot call hasNext");
    }
    return mPathComponents;
  }

  // TODO this implementation might be problematic
  // need to complete it later.
  public void skipChildrenOfTheCurrent() {
    if (mHasNextCalled) {
      throw new IllegalStateException("Cannot call hasNext");
    }
    mIteratorStack.pop().close();
    mPathComponents.remove(mPathComponents.size() - 1);
  }

  @Override
  public boolean hasNext() {
    while (!mIteratorStack.isEmpty() && !tryOnIterator(
        mIteratorStack.peek(), CloseableIterator::hasNext
    )) {
      mIteratorStack.pop().close();
      // When the iteration finishes, the size of mPathComponents is 0
      if (mPathComponents.size() > 0) {
        mPathComponents.remove(mPathComponents.size() - 1);
      }
    }
    mHasNextCalled = true;
    return !mIteratorStack.isEmpty();
  }

  @Override
  public Inode next() {
    Inode result = tryOnIterator(mIteratorStack.peek(), CloseableIterator::next);
    if (mRecursive) {
      ReadOption readOption = ReadOption.newBuilder()
          .setReadFrom(populateReadAfter(result.getName())).build();
      CloseableIterator<? extends Inode> nextLevelIterator =
          mInodeStore.getChildren(result.getId(), readOption);
      mIteratorStack.push(nextLevelIterator);
    }
    mCurrent = result;
    mPathComponents.add(mCurrent.getName());
    mHasNextCalled = false;
    return mCurrent;
  }

  // TODO add comments
  private @Nullable String populateReadAfter(String currentInodeName) {
    if (mPathComponents.size() + 1 >= mStartAfterPathComponents.size()) {
      return null;
    }
    for (int i = 0; i < mPathComponents.size(); ++i) {
      if (!mPathComponents.get(i).equals(mStartAfterPathComponents.get(i))) {
        return null;
      }
    }
    if (!currentInodeName.equals(mStartAfterPathComponents.get(mPathComponents.size()))) {
      return null;
    }
    return mStartAfterPathComponents.get(mPathComponents.size() + 1);
  }

  private <T> T tryOnIterator(CloseableIterator<? extends Inode> iterator, Function<CloseableIterator<? extends Inode>, T> supplier) {
    try {
      return supplier.apply(iterator);
    }  catch (Exception e) {
      iterator.close();
      throw e;
    }
  }
}