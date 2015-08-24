package tachyon.client.next.file;

import tachyon.client.next.ClientContext;
import tachyon.client.next.block.BlockMasterClientPool;
import tachyon.master.MasterClient;

/**
 * A shared context in each client JVM for common File System client functionality such as a pool
 * of master clients.
 */
public enum FSContext {
  INSTANCE;

  // TODO: Separate this when block master and file system master use different clients
  private final BlockMasterClientPool mFileSystemMasterClientPool;

  private FSContext() {
    mFileSystemMasterClientPool =
        new BlockMasterClientPool(ClientContext.getMasterAddress(), ClientContext.getConf());
  }

  public MasterClient acquireMasterClient() {
    return mFileSystemMasterClientPool.acquire();
  }

  public void releaseMasterClient(MasterClient masterClient) {
    mFileSystemMasterClientPool.release(masterClient);
  }
}
