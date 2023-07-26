package alluxio.master.job;

import java.util.OptionalInt;

public final class HashKey {
  private String mUFSPath;
  private OptionalInt mPartitionIndex;

  public OptionalInt getPartitionIndex() {
    return mPartitionIndex;
  }

  public HashKey(String ufsPath, OptionalInt partitionIndex) {
    mUFSPath = ufsPath;
    mPartitionIndex = partitionIndex;
  }

  public String getUFSPath() {
    return mUFSPath;
  }

  @Override
  public String toString() {
    if (mPartitionIndex.isPresent()&&mPartitionIndex.getAsInt()!=0) {
      return mUFSPath + ":" + mPartitionIndex.getAsInt();
    } else {
      return mUFSPath;
    }
  }

  public boolean isMetadata(){
    return !mPartitionIndex.isPresent();
  }

}
