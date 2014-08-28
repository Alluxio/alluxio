package tachyon;

import java.io.File;
import java.io.IOException;

/**
 * The mock cluster for local file system as UnderFileSystemSingleLocal.
 */
public class LocalFilesystemCluster extends UnderFileSystemCluster {

  public LocalFilesystemCluster(String baseDir) {
    super(baseDir);
  }

  @Override
  public String getUnderFilesystemAddress() {
    return new File(mBaseDir).getAbsolutePath();
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public void shutdown() throws IOException {
  }

  @Override
  public void start() throws IOException {
  }
}
