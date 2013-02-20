package tachyon;

public class InodeRawTable extends InodeFolder {
  private static final long serialVersionUID = -6912568260566139359L;

  private final int COLUMNS;

  public InodeRawTable(String name, int id, int parentId, int columns) {
    super(name, id, parentId, true);
    COLUMNS = columns;
  }

  public int getColumns() {
    return COLUMNS;
  }
}
