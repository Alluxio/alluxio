package tachyon;

import java.util.ArrayList;
import java.util.List;

public class InodeRawTable extends InodeFolder {
  private static final long serialVersionUID = -6912568260566139359L;

  private final int COLUMNS;

  private final List<Byte> METADATA;

  public InodeRawTable(String name, int id, int parentId, int columns, List<Byte> metadata) {
    super(name, id, parentId, true);
    COLUMNS = columns;
    if (metadata == null) {
      METADATA = new ArrayList<Byte>(0);
    } else {
      METADATA = new ArrayList<Byte>(metadata.size());
      for (int k = 0; k < metadata.size(); k ++) {
        METADATA.add(metadata.get(k));
      }
    }
  }

  public int getColumns() {
    return COLUMNS;
  }

  public List<Byte> getMetadata() {
    List<Byte> ret = new ArrayList<Byte>(METADATA.size());
    for (int k = 0; k < METADATA.size(); k ++) {
      ret.add(METADATA.get(k));
    }
    return ret;
  } 
}