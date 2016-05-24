/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.client.ClientContext;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockStoreContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.resource.CloseableResource;
import alluxio.util.IdUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A shared context in each client JVM for common file master client functionality such as a pool of
 * master clients. Any remote clients will be created and destroyed on a per use basis.
 * <p>
 * NOTE: The context maintains a pool of file system master clients that is already thread-safe.
 * Synchronizing {@link FileSystemContext} methods could lead to deadlock: thread A attempts to
 * acquire a client when there are no clients left in the pool and blocks holding a lock on the
 * {@link FileSystemContext}, when thread B attempts to release a client it owns it is unable to do
 * so, because thread A holds the lock on {@link FileSystemContext}.
 */
@ThreadSafe
public enum FileSystemContext {
  INSTANCE;

  private FileSystemMasterClientPool mFileSystemMasterClientPool;
  private final AlluxioBlockStore mAlluxioBlockStore;

  /** A list of valid workers, if there is a local worker, only the local worker addresses. */
  @GuardedBy("mWorkerAddressesLock")
  private List<WorkerNetAddress> mWorkerAddresses;
  /** Lock for mWorkerAddresses. */
  private final Object mWorkerAddressesLock = new Object();

  /**
   * Creates a new file stream context.
   */
  FileSystemContext() {
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(ClientContext.getMasterAddress());
    mAlluxioBlockStore = AlluxioBlockStore.get();
  }

  /**
   * Acquires a block master client from the block master client pool.
   *
   * @return the acquired block master client
   */
  public FileSystemMasterClient acquireMasterClient() {
    return mFileSystemMasterClientPool.acquire();
  }

  /**
   * Creates a new file system worker client, prioritizing local workers if available. This method
   * initializes the list of worker addresses if it has not been initialized.
   *
   * @return a file system worker client to a worker in the system
   * @throws IOException if an error occurs getting the list of workers in the system
   */
  public FileSystemWorkerClient createWorkerClient() throws IOException {
    WorkerNetAddress address;
    synchronized (mWorkerAddressesLock) {
      if (mWorkerAddresses == null) {
        mWorkerAddresses = getWorkerAddresses();
      }
      address = mWorkerAddresses.get(ThreadLocalRandom.current().nextInt(mWorkerAddresses.size()));
    }
    long sessionId = IdUtils.getRandomNonNegativeLong();
    return new FileSystemWorkerClient(address, ClientContext.getFileClientExecutorService(),
        ClientContext.getConf(), sessionId, ClientContext.getClientMetrics());
  }

  /**
   * Releases a block master client into the block master client pool.
   *
   * @param masterClient a block master client to release
   */
  public void releaseMasterClient(FileSystemMasterClient masterClient) {
    mFileSystemMasterClientPool.release(masterClient);
  }

  /**
   * @return the Alluxio block store
   */
  public AlluxioBlockStore getAlluxioBlockStore() {
    return mAlluxioBlockStore;
  }

  /**
   * Re-initializes the Block Store context. This method should only be used in
   * {@link ClientContext}.
   */
  public void reset() {
    mFileSystemMasterClientPool.close();
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(ClientContext.getMasterAddress());
    synchronized (mWorkerAddressesLock) {
      mWorkerAddresses = null;
    }
  }

  /**
   * @return if there are any local workers, the returned list will ONLY contain the local workers,
   *         otherwise a list of all remote workers will be returned
   * @throws IOException if an error occurs communicating with the master
   */
  private List<WorkerNetAddress> getWorkerAddresses() throws IOException {
    List<WorkerInfo> infos;
    try (CloseableResource<BlockMasterClient> masterClientResource =
        BlockStoreContext.INSTANCE.acquireMasterClientResource()) {
      infos = masterClientResource.get().getWorkerInfoList();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    if (infos.isEmpty()) {
      throw new IOException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }

    // Convert the worker infos into net addresses, if there are local addresses, only keep those
    List<WorkerNetAddress> workerNetAddresses = new ArrayList<>();
    List<WorkerNetAddress> localWorkerNetAddresses = new ArrayList<>();
    String localHostname = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
    for (WorkerInfo info : infos) {
      WorkerNetAddress netAddress = info.getAddress();
      if (netAddress.getHost().equals(localHostname)) {
        localWorkerNetAddresses.add(netAddress);
      }
      workerNetAddresses.add(netAddress);
    }

    return localWorkerNetAddresses.isEmpty() ? workerNetAddresses : localWorkerNetAddresses;
  }
}
