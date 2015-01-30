package tachyon.underfs.local;

import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemFactory;

public class SingleMachineLocalUnderFileSystemFactory implements UnderFileSystemFactory {

  @Override
  public boolean supportsPath(String path, TachyonConf conf) {
    if (path == null) {
      return false;
    }
    return path.startsWith(TachyonURI.SEPARATOR) || path.startsWith("file://");
  }

  @Override
  public UnderFileSystem create(String path, TachyonConf tachyonConf, Object conf) {
    Preconditions.checkArgument(path != null, "path may not be null");
    return new SingleMachineLocalUnderFileSystem(tachyonConf);
  }
}
