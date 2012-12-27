package tachyon.client;

import java.util.List;

import tachyon.CommonUtils;
import tachyon.thrift.PartitionInfo;
import tachyon.thrift.RawColumnDatasetInfo;

public class RawColumnDataset {
  private final TachyonClient mTachyonClient;

  private RawColumnDatasetInfo mRawColumnDatasetInfo;

  public RawColumnDataset(TachyonClient tachyonClient, RawColumnDatasetInfo rawColumnDatasetInfo) {
    mTachyonClient = tachyonClient;
    mRawColumnDatasetInfo = rawColumnDatasetInfo;
  }

  public List<Integer> getColumnDatasetIds() {
    return mRawColumnDatasetInfo.mColumnDatasetIdList;
  }

  public RCDPartition getPartition(int pId) {
    validatePartitionId(pId);
    return new RCDPartition(mTachyonClient, this, mRawColumnDatasetInfo.mId, pId);
  }

  public int getNumColumns() {
    return mRawColumnDatasetInfo.mColumns;
  }

  public int getNumPartitions() {
    return mRawColumnDatasetInfo.mNumOfPartitions;
  }

  public int getDatasetId() {
    return mRawColumnDatasetInfo.mId;
  }

  public String getDatasetName() {
    return mRawColumnDatasetInfo.mPath;
  }

  public PartitionInfo getPartitionInfo(int pId) {
    validatePartitionId(pId);
    return mRawColumnDatasetInfo.mPartitionList.get(pId);
  }

  public String getPath() {
    return mRawColumnDatasetInfo.mPath;
  }

  private void validatePartitionId(int pId) {
    if (pId < 0 || pId >= mRawColumnDatasetInfo.mNumOfPartitions) {
      CommonUtils.runtimeException(mRawColumnDatasetInfo.mPath + " does not have partition " + pId
          + ". It has " + mRawColumnDatasetInfo.mNumOfPartitions + " partitions.");
    }
  }
}