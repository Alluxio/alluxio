package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.underfs.local.LocalUnderFileSystem;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * A factory to create a {@link LocalUnderFileSystem} that requires expected conf, or throws
 * exceptions during creation. On success, a local under file system is created.
 */
public final class ConfExpectingUnderFileSystemFactory implements UnderFileSystemFactory {
  /** The scheme of this UFS. */
  public final String mScheme;
  /** The conf expected to create the ufs. */
  private final Map<String, String> mExpectedConf;

  public ConfExpectingUnderFileSystemFactory(String scheme, Map<String, String> expectedConf) {
    mScheme = scheme;
    mExpectedConf = expectedConf;
  }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkArgument(mExpectedConf.equals(conf.getUserSpecifiedConf()),
        "ufs conf {} does not match expected {}", conf, mExpectedConf);
    return new LocalUnderFileSystem(new AlluxioURI(new AlluxioURI(path).getPath()));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(mScheme + ":///");
  }
}
