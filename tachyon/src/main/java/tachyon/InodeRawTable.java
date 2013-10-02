package tachyon;

import java.nio.ByteBuffer;

import tachyon.thrift.TachyonException;

/**
 * Tachyon file system's RawTable representation in master.
 */
public class InodeRawTable extends InodeFolder {
  protected final int COLUMNS;

  private ByteBuffer mMetadata;

  public InodeRawTable(String name, int id, int parentId, int columns, ByteBuffer metadata,
      long creationTimeMs) throws TachyonException {
    super(name, id, parentId, InodeType.RawTable, creationTimeMs);
    COLUMNS = columns;
    updateMetadata(metadata);
  }

  public int getColumns() {
    return COLUMNS;
  }

  // TODO add version number.
  public synchronized void updateMetadata(ByteBuffer metadata) throws TachyonException {
    if (metadata == null) {
      mMetadata = ByteBuffer.allocate(0);
    } else {
      if (metadata.limit() - metadata.position() >= Constants.MAX_TABLE_METADATA_BYTE) {
        throw new TachyonException("Too big table metadata: " + metadata.toString());
      }
      mMetadata = ByteBuffer.allocate(metadata.limit() - metadata.position());
      mMetadata.put(metadata.array(), metadata.position(), metadata.limit() - metadata.position());
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