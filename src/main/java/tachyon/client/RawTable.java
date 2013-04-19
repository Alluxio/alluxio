package tachyon.client;

import java.nio.ByteBuffer;

import tachyon.CommonUtils;
import tachyon.thrift.ClientRawTableInfo;

/**
 * Tachyon provides native support for tables with multiple columns. Each table contains one or
 * more columns. Each columns contains one or more ordered files.
 */
public class RawTable {
  private final TachyonClient TACHYON_CLIENT;
  private final ClientRawTableInfo CLIENT_RAW_TABLE_INFO;

  RawTable(TachyonClient tachyonClient, ClientRawTableInfo clientRawTableInfo) {
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
    ByteBuffer ret = CLIENT_RAW_TABLE_INFO.metadata.duplicate();
    return ret.asReadOnlyBuffer();
  }

  public RawColumn getRawColumn(int columnIndex) {
    if (columnIndex < 0 || columnIndex >= CLIENT_RAW_TABLE_INFO.getColumns()) {
      CommonUtils.runtimeException(CLIENT_RAW_TABLE_INFO.getPath() + " does not have column " + 
          columnIndex + ". It has " + CLIENT_RAW_TABLE_INFO.getColumns() + " columns.");
    }

    return new RawColumn(TACHYON_CLIENT, this, columnIndex);
  }
}