package tachyon.client;

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.MasterInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * The column of a <code>RawTable</code>.
 */
public class RawColumn {
  private final TachyonFS TACHYON_CLIENT;
  private final RawTable RAW_TABLE;
  private final int COLUMN_INDEX;

  RawColumn(TachyonFS tachyonClient, RawTable rawTable, int columnIndex) {
    TACHYON_CLIENT = tachyonClient;
    RAW_TABLE = rawTable;
    COLUMN_INDEX = columnIndex;
  }

  // TODO creating file here should be based on id.
  public boolean createPartition(int pId) throws InvalidPathException, FileAlreadyExistException {
    return TACHYON_CLIENT.createFile(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR + 
        MasterInfo.COL + COLUMN_INDEX + Constants.PATH_SEPARATOR + pId) > 0;
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId) throws InvalidPathException {
    return getPartition(pId, false);
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId, boolean cachedMetadata) throws InvalidPathException {
    return TACHYON_CLIENT.getFile(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR + MasterInfo.COL +
        COLUMN_INDEX + Constants.PATH_SEPARATOR + pId, cachedMetadata);
  }

  // TODO creating file here should be based on id.
  public int partitions() throws FileDoesNotExistException, InvalidPathException, TException {
    return TACHYON_CLIENT.getNumberOfFiles(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR +
        MasterInfo.COL + COLUMN_INDEX);
  }
}