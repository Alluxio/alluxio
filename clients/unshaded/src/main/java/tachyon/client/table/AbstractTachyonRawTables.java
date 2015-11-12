package tachyon.client.table;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.RawTableMasterClient;
import tachyon.exception.TachyonException;
import tachyon.thrift.RawTableInfo;

import java.io.IOException;
import java.nio.ByteBuffer;

// TODO(calvin): Consider different client options
@PublicApi
public class AbstractTachyonRawTables implements TachyonRawTablesCore {
  protected RawTablesContext mContext;

  protected AbstractTachyonRawTables() {
    mContext = RawTablesContext.INSTANCE;
  }

  @Override
  public SimpleRawTable create(TachyonURI path, int numColumns, ByteBuffer metadata) throws
      IOException, TachyonException {
    RawTableMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long rawTableId = masterClient.createRawTable(path, numColumns, metadata);
      return new SimpleRawTable(rawTableId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public RawTableInfo getInfo(SimpleRawTable rawTable) throws IOException, TachyonException {
    RawTableMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getClientRawTableInfo(rawTable.getRawTableId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public SimpleRawTable open(TachyonURI path) throws IOException, TachyonException {
    RawTableMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long rawTableId = masterClient.getClientRawTableInfo(path).getId();
      return new SimpleRawTable(rawTableId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void updateRawTableMetadata(SimpleRawTable rawTable, ByteBuffer metadata) throws IOException,
      TachyonException {
    RawTableMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.updateRawTableMetadata(rawTable.getRawTableId(), metadata);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
