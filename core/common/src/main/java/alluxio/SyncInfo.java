package alluxio;

import java.util.List;

public class SyncInfo {
  private List<AlluxioURI> mSyncList;
  private List<AlluxioURI> mFileUpdateList;

  public SyncInfo(List<AlluxioURI> syncList, List<AlluxioURI> fileUpdateList) {
    mSyncList = syncList;
    mFileUpdateList = fileUpdateList;
  }

  public List<AlluxioURI> getSyncList() {
    return mSyncList;
  }

  public List<AlluxioURI> getFileUpdateList(){
    return mFileUpdateList;
  }
}
