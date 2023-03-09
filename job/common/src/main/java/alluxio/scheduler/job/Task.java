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

package alluxio.scheduler.job;

import alluxio.client.block.stream.BlockWorkerClient;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * A task that can be executed on a worker. Belongs to a {@link Job}.
 *
 * @param <V> the response type of the task
 */
public abstract class Task<V> {

  /**
   * run the task.
   */
  protected abstract ListenableFuture<V> run(BlockWorkerClient client);

  private ListenableFuture<V> mResponseFuture;

  /**
   * @return the response future
   */
  public ListenableFuture<V> getResponseFuture() {
    return mResponseFuture;
  }

  /**
   * run the task and set the response future.
   * @param client worker client
   */
  public void execute(BlockWorkerClient client) {
    mResponseFuture = run(client);
  }
}
