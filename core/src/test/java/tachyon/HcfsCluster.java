package tachyon;

import java.io.IOException;
import com.google.common.base.Throwables;

/**
 * This implements Hadoop compatible filesystem (except HDFS) cluster 
 * to test under filesystems like glusterfs and cephfs.
 */
public class HcfsCluster extends UnderFileSystemCluster {
  private final String HCFS_URI_KEY = "uri";
  private final String mUri;

  public HcfsCluster(String baseDir) {
    super(baseDir);
    String uri = System.getProperty(HCFS_URI_KEY);
    if (null != uri && !uri.equals("")) {
      mUri = uri;
    }else {
    	mUri = null;
    	throw Throwables.propagate(new Exception("Invalid HCFS URI"));
    }
  }

  @Override
  public String getUnderFilesystemAddress() {
	if ('/' == mUri.charAt(mUri.length() - 1)) { 
		return mUri + "tachyon_test";
	}
	return mUri + "/tachyon_test";
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
