package tachyon;

import java.io.IOException;

/**
 * The implements Hadoop compatible filesystem cluster 
 * to test under filesystems like glusterfs and cephfs.
 */
public class HcfsCluster extends UnderFileSystemCluster {
  private final String HCFS_URI_KEY = "uri";
  private String mUri = null;

  public HcfsCluster(String baseDir) {
    super(baseDir);
    String uri = System.getProperty(HCFS_URI_KEY);
    if (null != uri && !uri.equals("")) {
      mUri = uri;
    }
  }

  @Override
  public String getUnderFilesystemAddress() {
    return mUri + "tachyon_test";
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public void shutdown() throws IOException {}

  @Override
  public void start() throws IOException {}
}
