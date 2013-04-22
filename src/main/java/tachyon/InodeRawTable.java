package tachyon;

import java.nio.ByteBuffer;

/**
 * Tachyon file system's RawTable representation in master.
 */
public class InodeRawTable extends InodeFolder {
  protected final int COLUMNS;

  private ByteBuffer mMetadata;

  public InodeRawTable(String name, int id, int parentId, int columns, ByteBuffer metadata) {
    super(name, id, parentId, InodeType.RawTable);
    COLUMNS = columns;
    updateMetadata(metadata);
  }

  public int getColumns() {
    return COLUMNS;
  }

  // TODO add version number.
  public synchronized void updateMetadata(ByteBuffer metadata) {
    if (metadata == null) {
      mMetadata = ByteBuffer.allocate(0);
    } else {
      mMetadata = ByteBuffer.allocate(metadata.limit());
      mMetadata.put(metadata);
      mMetadata.flip();
    }
  }

  public synchronized ByteBuffer getMetadata() {
    ByteBuffer ret = ByteBuffer.allocate(mMetadata.capacity());
    ret.put(mMetadata.array());
    ret.flip();
    return ret;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeRawTable(");
    sb.append(super.toString()).append(",").append(COLUMNS).append(",");
    sb.append(mMetadata).append(")");
    return sb.toString();
  }
}