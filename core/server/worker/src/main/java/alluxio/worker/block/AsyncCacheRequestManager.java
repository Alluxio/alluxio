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
   * @param dataSource where the data can be retrieved from
   */
  public void submitRequest(long blockId, WorkerNetAddress dataSource) {

  }
}
