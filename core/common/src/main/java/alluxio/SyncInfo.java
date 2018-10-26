package alluxio;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SyncInfo {
  private List<AlluxioURI> mSyncList;
  private Map<AlluxioURI, Set<AlluxioURI>> mFileUpdateList;
  private static SyncInfo emptyInfo = new SyncInfo(Collections.emptyList(), Collections.emptyMap());


  public SyncInfo(List<AlluxioURI> syncList, Map<AlluxioURI, Set<AlluxioURI>> fileUpdateList) {
    mSyncList = syncList;
    mFileUpdateList = fileUpdateList;
  }

  public static SyncInfo emptyInfo() {
    return emptyInfo;
  }

  public List<AlluxioURI> getSyncList() {
    return mSyncList;
  }

  public Set<AlluxioURI> getFileUpdateList(AlluxioURI syncPoint){
    return mFileUpdateList.get(syncPoint);
  }
}
