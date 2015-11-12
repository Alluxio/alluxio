package tachyon.client.table;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.RawTableMasterClient;
import tachyon.resource.ResourcePool;

import java.net.InetSocketAddress;

public class RawTableMasterClientPool extends ResourcePool<RawTableMasterClient> {
  private final InetSocketAddress mMasterAddress;

  public RawTableMasterClientPool(InetSocketAddress masterAddress) {
    super(ClientContext.getConf().getInt(Constants.USER_RAW_TABLE_MASTER_CLIENT_THREADS));
    mMasterAddress = masterAddress;
  }

  @Override
  public void close() {
    // TODO(calvin): Consider collecting all the clients and shutting them down
  }

  @Override
  protected RawTableMasterClient createNewResource() {
    return new RawTableMasterClient(mMasterAddress, ClientContext.getConf());
  }
}
