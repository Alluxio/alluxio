package alluxio.worker.block;

import alluxio.wire.WorkerNetAddress;

/**
 * Handles client requests to asynchronously cache blocks. Responsible for managing the local
 * worker resources and intelligent pruning of duplicate or meaningless requests.
 */
public class AsyncCacheRequestManager {
  /**
   * Submits a request to cache the given block id.
   * @param blockId block to cache
   */
  public void submitRequest(long blockId, WorkerNetAddress dataSource) {

  }
}
