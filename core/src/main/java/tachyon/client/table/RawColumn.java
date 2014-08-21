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
  private final TachyonFS TFS;
  private final RawTable RAW_TABLE;
  private final int COLUMN_INDEX;

  /**
   * @param tachyonClient
   * @param rawTable
   * @param columnIndex
   */
  RawColumn(TachyonFS tachyonClient, RawTable rawTable, int columnIndex) {
    TFS = tachyonClient;
    RAW_TABLE = rawTable;
    COLUMN_INDEX = columnIndex;
  }

  // TODO creating file here should be based on id.
  public boolean createPartition(int pId) throws IOException {
    return TFS.createFile(CommonUtils.concat(RAW_TABLE.getPath(), MasterInfo.COL + COLUMN_INDEX,
        pId)) > 0;
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId) throws IOException {
    return getPartition(pId, false);
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId, boolean cachedMetadata) throws IOException {
    return TFS.getFile(
        CommonUtils.concat(RAW_TABLE.getPath(), MasterInfo.COL + COLUMN_INDEX, pId),
        cachedMetadata);
  }

  // TODO creating file here should be based on id.
  public int partitions() throws IOException {
    return TFS.getNumberOfFiles(CommonUtils.concat(RAW_TABLE.getPath(), MasterInfo.COL
        + COLUMN_INDEX));
  }
}
