package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.fuse.file.FuseFileInStream;
import alluxio.fuse.file.FuseFileStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for {@link FuseFileInStream}.
 */
@ThreadSafe
public interface FuseStreamFactory {

  /**
   * Factory method for creating/opening a file
   * and creating an implementation of {@link FuseFileStream}.
   *
   * @param uri the Alluxio URI
   * @param flags the create/open flags
   * @param mode the create file mode, -1 if not set
   * @return the created fuse file stream
   */
  FuseFileStream create(AlluxioURI uri, int flags, long mode);
}