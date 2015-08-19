package tachyon.client.next.block;

import com.google.common.base.Preconditions;
import tachyon.client.next.ClientContext;
import tachyon.master.MasterClient;
import tachyon.thrift.NetAddress;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.ClientMetrics;
import tachyon.worker.next.WorkerClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A shared context in each client JVM for common Block Store client functionality such as Master
 * client pools.
 */
public enum BSContext {
  INSTANCE;

  private final BlockMasterClientPool mBlockMasterClientPool;
  private final BlockWorkerClientPool mLocalBlockWorkerClientPool;
  private final ExecutorService mRemoteBlockWorkerExecutor;
  private final NetAddress mLocalWorkerAddress;

  private BSContext() {
    mBlockMasterClientPool =
        new BlockMasterClientPool(ClientContext.getMasterAddress(), ClientContext.getConf());
    // TODO: Get size from configuration
    mRemoteBlockWorkerExecutor =
        Executors.newFixedThreadPool(10,
            ThreadFactoryUtils.build("remote-block-worker-heartbeat-%d", true));

    mLocalWorkerAddress =
        getWorkerAddress(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()));

    // If the local worker is not available, do not initialize the local worker client pool
    if (null == mLocalWorkerAddress) {
      mLocalBlockWorkerClientPool = null;
    } else {
      mLocalBlockWorkerClientPool =
          new BlockWorkerClientPool(mLocalWorkerAddress, ClientContext.getConf());
    }
  }

  private NetAddress getWorkerAddress(String hostname) {
    MasterClient masterClient = acquireMasterClient();
    try {
      if (hostname.isEmpty()) {
        return masterClient.user_getWorker(true, hostname);
      }
      return masterClient.user_getWorker(false, hostname);
    } catch (Exception e) {
      return null;
    } finally {
      releaseMasterClient(masterClient);
    }
  }

  public MasterClient acquireMasterClient() {
    return mBlockMasterClientPool.acquire();
  }

  public void releaseMasterClient(MasterClient masterClient) {
    mBlockMasterClientPool.release(masterClient);
  }

  /**
   * Obtains a worker client to a worker in the system, or throws exception if no workers are
   * available. A local client is preferred to be returned but not guaranteed. The caller should
   * use {@link WorkerClient#isLocal} to verify if the client is local before assuming so.
   *
   * @return a WorkerClient to a worker in the Tachyon system
   */
  public WorkerClient acquireWorkerClient() {
    if (mLocalBlockWorkerClientPool != null) {
      return mLocalBlockWorkerClientPool.acquire();
    } else {
      return acquireRemoteWorkerClient("");
    }
  }

  public WorkerClient acquireWorkerClient(String hostname) {
    if (hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
      if (mLocalBlockWorkerClientPool != null) {
        return mLocalBlockWorkerClientPool.acquire();
      }
      // TODO: Recover from initial worker failure
      throw new RuntimeException("No Tachyon worker available for host: " + hostname);
    }
    return acquireRemoteWorkerClient(hostname);
  }

  private WorkerClient acquireRemoteWorkerClient(String hostname) {
    Preconditions.checkArgument(
        !hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf())),
        "Acquire Remote Worker Client cannot not be called with local hostname");
    NetAddress workerAddress = getWorkerAddress(hostname);

    // If we couldn't find a worker, crash
    if (null == workerAddress) {
      // TODO: Better exception usage
      throw new RuntimeException("No Tachyon worker available for host: " + hostname);
    }
    // TODO: Get the id from the master
    long clientId = Math.round(Math.random() * 100000);
    return new WorkerClient(workerAddress, mRemoteBlockWorkerExecutor, ClientContext.getConf(),
        clientId, new ClientMetrics());
  }

  public void releaseWorkerClient(WorkerClient workerClient) {
    // If the client is local and the pool exists, release the client to the pool, otherwise just
    // close the client
    if (mLocalBlockWorkerClientPool != null && workerClient.isLocal()) {
      mLocalBlockWorkerClientPool.release(workerClient);
    } else {
      workerClient.close();
    }
  }

  public boolean hasLocalWorker() {
    return mLocalBlockWorkerClientPool != null;
  }
}
