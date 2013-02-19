package tachyon.client;

public class RawColumn {
  private final TachyonClient TACHYON_CLIENT;
  private final RawTable RAW_TABLE;
  private final int COLUMN_INDEX;

  public RawColumn(TachyonClient tachyonClient, RawTable rawTable, int columnIndex) {
    // TODO Auto-generated constructor stub
    TACHYON_CLIENT = tachyonClient;
    RAW_TABLE = rawTable;
    COLUMN_INDEX = columnIndex;
  }

}