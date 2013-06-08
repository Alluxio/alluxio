package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.CommonUtils;
import tachyon.thrift.ClientRawTableInfo;

/**
 * Tachyon provides native support for tables with multiple columns. Each table contains one or
 * more columns. Each columns contains one or more ordered files.
 */
public class RawTable {
  private final TachyonFS TACHYON_CLIENT;
  private final ClientRawTableInfo CLIENT_RAW_TABLE_INFO;

  RawTable(TachyonFS tachyonClient, ClientRawTableInfo clientRawTableInfo) {
    TACHYON_CLIENT = tachyonClient;
    CLIENT_RAW_TABLE_INFO = clientRawTableInfo;
  }

  public int getColumns() {
    return CLIENT_RAW_TABLE_INFO.getColumns();
  }

  public int getId() {
    return CLIENT_RAW_TABLE_INFO.getId();
  }

  public String getName() {
    return CLIENT_RAW_TABLE_INFO.getName();
  }

  public String getPath() {
    return CLIENT_RAW_TABLE_INFO.getPath();
  }

  public ByteBuffer getMetadata() {
    return CommonUtils.cloneByteBuffer(CLIENT_RAW_TABLE_INFO.metadata);
  }

  public RawColumn getRawColumn(int columnIndex) {
    if (columnIndex < 0 || columnIndex >= CLIENT_RAW_TABLE_INFO.getColumns()) {
      CommonUtils.runtimeException(CLIENT_RAW_TABLE_INFO.getPath() + " does not have column " + 
          columnIndex + ". It has " + CLIENT_RAW_TABLE_INFO.getColumns() + " columns.");
    }

    return new RawColumn(TACHYON_CLIENT, this, columnIndex);
  }

  public void updateMetadata(ByteBuffer metadata) throws IOException {
    TACHYON_CLIENT.updateRawTableMetadata(CLIENT_RAW_TABLE_INFO.getId(), metadata);
    CLIENT_RAW_TABLE_INFO.setMetadata(CommonUtils.cloneByteBuffer(metadata));
  }
}