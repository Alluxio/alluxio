package tachyon.client;

public class RawColumn {
  private final TachyonClient TACHYON_CLIENT;
  private final RawTable RAW_TABLE;
  private final int COLUMN_INDEX;

  public RawColumn(TachyonClient tachyonClient, RawTable rawTable, int columnIndex) {
    TACHYON_CLIENT = tachyonClient;
    RAW_TABLE = rawTable;
    COLUMN_INDEX = columnIndex;
  }
  
  public boolean createPartition(int pId) {
    return TACHYON_CLIENT.createFile(RAW_TABLE.getPath() + "/" + COLUMN_INDEX + "/" + pId) > 0;
  }
  
  public TachyonFile getPartition(int pId) {
    return TACHYON_CLIENT.getFile(RAW_TABLE.getPath() + "/" + COLUMN_INDEX + "/" + pId);
  }
}