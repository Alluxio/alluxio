package tachyon.client.table;

import java.io.IOException;

import tachyon.TachyonURI;
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
    TachyonURI tUri =
        new TachyonURI(CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex, pId));
    return mTachyonFS.createFile(tUri) > 0;
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId) throws IOException {
    return getPartition(pId, false);
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId, boolean cachedMetadata) throws IOException {
    TachyonURI tUri =
        new TachyonURI(CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex, pId));
    return mTachyonFS.getFile(tUri, cachedMetadata);
  }

  // TODO creating file here should be based on id.
  public int partitions() throws IOException {
    TachyonURI tUri =
        new TachyonURI(CommonUtils.concat(mRawTable.getPath(), MasterInfo.COL + mColumnIndex));
    return mTachyonFS.listStatus(tUri).size();
  }
}
