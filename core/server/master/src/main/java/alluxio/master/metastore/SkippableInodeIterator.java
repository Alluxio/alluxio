package alluxio.master.metastore;

import alluxio.master.file.meta.InodeIterationResult;
import alluxio.resource.CloseableResource;

import java.io.Closeable;
import java.util.Iterator;

public interface SkippableInodeIterator
    extends Iterator<InodeIterationResult>, Closeable {
  /**
   * Skip the children of the current inode during the iteration
   */
  default void skipChildrenOfTheCurrent() {
    throw new UnsupportedOperationException("Operation not supported");
  }
}
