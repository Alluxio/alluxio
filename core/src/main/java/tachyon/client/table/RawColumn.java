package tachyon.client.table;

import java.io.IOException;

import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.master.MasterInfo;
import tachyon.util.CommonUtils;

/**
 * The column of a <code>RawTable</code>.
 */
public class RawColumn {
  private final TachyonFS mTachyonFS;
  private final RawTable mRawTable;
  private final int mColumnIndex;

  /**
   * @param tachyonClient
   * @param rawTable
   * @param columnIndex
   */
  RawColumn(TachyonFS tachyonClient, RawTable rawTable, int columnIndex) {
    mTachyonFS = tachyonClient;
    mRawTable = rawTable;
    mColumnIndex = columnIndex;
  }

  // TODO creating file here should be based on id.
  public boolean createPartition(int pId) throws IOException {
    return mTachyonFS.createFile(
        CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex, pId)) > 0;
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId) throws IOException {
    return getPartition(pId, false);
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId, boolean cachedMetadata) throws IOException {
    return mTachyonFS.getFile(
        CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex, pId),
        cachedMetadata);
  }

  // TODO creating file here should be based on id.
  public int partitions() throws IOException {
    return mTachyonFS.listStatus(
        CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex)).size();
  }
}
