package tachyon;

import java.io.IOException;

public class GlusterfsCluster extends UnderFileSystemCluster {

  public GlusterfsCluster(String baseDir) {
    super(baseDir);
  }

  @Override
  public String getUnderFilesystemAddress() {
    return "glusterfs:///tachyon_test";
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