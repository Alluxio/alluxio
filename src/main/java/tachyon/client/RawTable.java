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

  private ClientRawTableInfo mClientRawTableInfo;

  public RawTable(TachyonClient tachyonClient, ClientRawTableInfo clientRawTableInfo) {
    TACHYON_CLIENT = tachyonClient;
    mClientRawTableInfo = clientRawTableInfo;
  }

  public int getColumns() {
    return mClientRawTableInfo.getColumns();
  }

  public int getId() {
    return mClientRawTableInfo.getId();
  }

  public String getName() {
    return mClientRawTableInfo.getName();
  }

  public String getPath() {
    return mClientRawTableInfo.getPath();
  }

  public ByteBuffer getMetadata() {
    ByteBuffer ret = mClientRawTableInfo.metadata.duplicate();
    ret.asReadOnlyBuffer();
    return ret;
  }

  public RawColumn getRawColumn(int columnIndex) {
    if (columnIndex < 0 || columnIndex >= mClientRawTableInfo.getColumns()) {
      CommonUtils.runtimeException(mClientRawTableInfo.getPath() + " does not have column " + 
          columnIndex + ". It has " + mClientRawTableInfo.getColumns() + " columns.");
    }

    return new RawColumn(TACHYON_CLIENT, this, columnIndex);
  }
}