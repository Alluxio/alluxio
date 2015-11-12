package tachyon.client.table;

import tachyon.client.ClientContext;
import tachyon.client.RawTableMasterClient;

public enum RawTablesContext {
  INSTANCE;

  private RawTableMasterClientPool mRawTableMasterClientPool;

  RawTablesContext() {
    mRawTableMasterClientPool = new RawTableMasterClientPool(ClientContext.getMasterAddress());
  }

  public RawTableMasterClient acquireMasterClient() {
    return mRawTableMasterClientPool.acquire();
  }

  public void releaseMasterClient(RawTableMasterClient masterClient) {
    mRawTableMasterClientPool.release(masterClient);
  }

  // TODO(calvin): Rethink resetting contexts outside of test cases
  public void reset() {
    mRawTableMasterClientPool.close();
    mRawTableMasterClientPool = new RawTableMasterClientPool(ClientContext.getMasterAddress());
  }
}
