package tachyon;

import java.nio.ByteBuffer;

public class InodeRawTable extends InodeFolder {
  private static final long serialVersionUID = -6912568260566139359L;

  private final int COLUMNS;

  private final byte[] METADATA;

  public InodeRawTable(String name, int id, int parentId, int columns) {
    this(name, id, parentId, columns, new byte[0]);
  }

  public InodeRawTable(String name, int id, int parentId, int columns, byte[] metadata) {
    super(name, id, parentId, true);
    COLUMNS = columns;
    METADATA = new byte[metadata.length];
    for (int k = 0; k < metadata.length; k ++) {
      METADATA[k] = metadata[k];
    }
  }

  public int getColumns() {
    return COLUMNS;
  }

  public ByteBuffer getMetadata() {
    return ByteBuffer.wrap(METADATA).asReadOnlyBuffer();
  }
}
