package tachyon.client;

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.MasterInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

public class RawColumn {
  private final TachyonClient TACHYON_CLIENT;
  private final RawTable RAW_TABLE;
  private final int COLUMN_INDEX;

  public RawColumn(TachyonClient tachyonClient, RawTable rawTable, int columnIndex) {
    TACHYON_CLIENT = tachyonClient;
    RAW_TABLE = rawTable;
    COLUMN_INDEX = columnIndex;
  }

  // TODO creating file here should be based on id.
  public boolean createPartition(int pId) throws InvalidPathException, FileAlreadyExistException {
    return TACHYON_CLIENT.createFile(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR + 
        MasterInfo.COL + COLUMN_INDEX + "/" + pId) > 0;
  }

  // TODO creating file here should be based on id.
  public TachyonFile getPartition(int pId) throws InvalidPathException {
    return TACHYON_CLIENT.getFile(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR + MasterInfo.COL +
        COLUMN_INDEX + "/" + pId);
  }

  // TODO creating file here should be based on id.
  public int getPartitions() throws FileDoesNotExistException, InvalidPathException, TException {
    return TACHYON_CLIENT.getNumberOfFiles(RAW_TABLE.getPath() + Constants.PATH_SEPARATOR +
        MasterInfo.COL + COLUMN_INDEX);
  }
}