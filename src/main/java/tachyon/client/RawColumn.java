package tachyon.client;

import java.io.IOException;

import tachyon.Constants;
import tachyon.MasterInfo;

/**
 * The column of a <code>RawTable</code>.
 */
public class RawColumn {
  private final TachyonFS TFS;
  private final RawTable RAW_TABLE;
  private final int COLUMN_INDEX;

  RawColumn(TachyonFS tachyonClient, RawTable rawTable, int columnIndex) {
    TFS = tachyonClient;
    RAW_TABLE = rawTable;
    COLUMN_INDEX = columnIndex;
  }

  // TODO creating file here should be based on id.
  public boolean createPartition(int pId) throws IOException {
    return TFS.createFile(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR + 
        MasterInfo.COL + COLUMN_INDEX + Constants.PATH_SEPARATOR + pId) > 0;
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId) throws IOException {
    return getPartition(pId, false);
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId, boolean cachedMetadata) {
    return TFS.getFile(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR + MasterInfo.COL +
        COLUMN_INDEX + Constants.PATH_SEPARATOR + pId, cachedMetadata);
  }

  // TODO creating file here should be based on id.
  public int partitions() throws IOException {
    return TFS.getNumberOfFiles(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR +
        MasterInfo.COL + COLUMN_INDEX);
  }
}