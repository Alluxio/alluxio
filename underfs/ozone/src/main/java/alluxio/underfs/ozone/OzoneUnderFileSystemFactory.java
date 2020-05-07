package alluxio.underfs.ozone;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link OzoneUnderFileSystem}.
 *
 * It caches created {@link OzoneUnderFileSystem}s, using the scheme and authority pair as the key.
 */
@ThreadSafe
public class OzoneUnderFileSystemFactory implements UnderFileSystemFactory {
  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    if (checkOzoneCredentials(conf)) {
      try {
        return OzoneUnderFileSystem.createInstance(new AlluxioURI(path), conf);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "COS Credentials not available, cannot create COS Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_OZONE);
  }

  /**
   * @param conf optional configuration object for the UFS
   *
   * @return true if both access, secret and endpoint keys are present, false otherwise
   */
  private boolean checkOzoneCredentials(UnderFileSystemConfiguration conf) {
    // TODO(maobaolong): impl check logic while ozone supported
    return true;
  }
}
