package alluxio;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to represent what the active syncing process should sync.
 */
public class SyncInfo {
  // A map mapping syncpoints to files changed in those sync points
  private Map<AlluxioURI, Set<AlluxioURI>> mFileUpdateList;
  private static final SyncInfo EMPTY_INFO = new SyncInfo(Collections.emptyMap());

  public SyncInfo(Map<AlluxioURI, Set<AlluxioURI>> fileUpdateList) {
    mFileUpdateList = fileUpdateList;
  }

  public static SyncInfo emptyInfo() {
    return EMPTY_INFO;
  }

  public Set<AlluxioURI> getSyncList() {
    return mFileUpdateList.keySet();
  }

  public Set<AlluxioURI> getFileUpdateList(AlluxioURI syncPoint){
    return mFileUpdateList.get(syncPoint);
  }
}
