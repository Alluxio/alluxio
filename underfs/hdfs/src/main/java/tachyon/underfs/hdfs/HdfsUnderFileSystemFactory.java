package tachyon.underfs.hdfs;

import com.google.common.base.Preconditions;

import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemFactory;

public class HdfsUnderFileSystemFactory implements UnderFileSystemFactory {

  @Override
  public boolean supportsPath(String path) {
    if (path == null) {
      return false;
    }

    return UnderFileSystem.isHadoopUnderFS(path);
  }

  @Override
  public UnderFileSystem create(String path, Object conf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    return new HdfsUnderFileSystem(path, conf);
  }

}
