package tachyon;

import java.nio.ByteBuffer;

public class InodeRawTable extends InodeFolder {
  protected final int COLUMNS;

  private final ByteBuffer METADATA;

  public InodeRawTable(String name, int id, int parentId, int columns, ByteBuffer metadata) {
    super(name, id, parentId, InodeType.RawTable);
    COLUMNS = columns;
    if (metadata == null) {
      METADATA = ByteBuffer.allocate(0);
    } else {
      METADATA = ByteBuffer.allocate(metadata.limit());
      METADATA.put(metadata);
    }
  }

  public int getColumns() {
    return COLUMNS;
  }

  public ByteBuffer getMetadata() {
    ByteBuffer ret = METADATA.duplicate();
    ret.asReadOnlyBuffer();
    return ret;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeRawTable(");
    sb.append(super.toString()).append(",").append(COLUMNS).append(")");
    return sb.toString();
  }
}