package alluxio.master.job;

import static java.util.Objects.requireNonNull;

import alluxio.AlluxioURI;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.wire.FileInfo;

import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Predicate;

public class UfsStatusIterable implements Iterable<UfsStatus> {

  private final UnderFileSystem mUfs;
  private final String mPath;
  private final Optional<String> mUser;
  private final Predicate<UfsStatus> mFilter;

  /**
   * Creates a new instance of {@link UfsStatusIterable}.
   *
   * @param fs                under file system
   * @param path              path to list
   * @param user              user to list as
   */
  public UfsStatusIterable(UnderFileSystem fs, String path, Optional<String> user,
      Predicate<UfsStatus> filter) {
    mUfs = requireNonNull(fs, "fileSystem is null");
    mPath = requireNonNull(path, "path is null");
    mUser = requireNonNull(user, "user is null");
    mFilter = filter;
  }

  @Override
  public Iterator<UfsStatus> iterator() {
    try {
      AuthenticatedClientUser.set(mUser.orElse(null));
      UfsStatus rootUfsStatus = mUfs.getStatus(mPath);
      if (rootUfsStatus != null && rootUfsStatus.isFile()) {
        rootUfsStatus.setUfsFullPath(new AlluxioURI(mPath));
        return Iterators.filter(Iterators.singletonIterator(rootUfsStatus),
            mFilter::test);
      }
      Iterator<UfsStatus> statuses =
          mUfs.listStatusIterable(mPath, ListOptions.defaults().setRecursive(true), null, 0);
      if (statuses == null) {
        throw new InternalRuntimeException("Get null when listing directory: " + mPath);
      }
      else {
        return Iterators.filter(statuses, mFilter::test);
      }
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }
}
