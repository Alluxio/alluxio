package tachyon.client;

import tachyon.CommonUtils;
import tachyon.thrift.ClientRawTableInfo;

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

  public RawColumn getRawColumn(int columnIndex) {
    if (columnIndex < 0 || columnIndex >= mClientRawTableInfo.getColumns()) {
      CommonUtils.runtimeException(mClientRawTableInfo.getPath() + " does not have column " + 
          columnIndex + ". It has " + mClientRawTableInfo.getColumns() + " columns.");
    }

    return new RawColumn(TACHYON_CLIENT, this, columnIndex);
  }
}