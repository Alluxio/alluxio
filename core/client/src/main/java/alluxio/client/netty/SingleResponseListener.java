/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.netty;

import alluxio.network.protocol.RPCResponse;

import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple listener that waits for a single {@link RPCResponse} message from the remote server.
 */
@NotThreadSafe
public final class SingleResponseListener implements ClientHandler.ResponseListener {

  private SettableFuture<RPCResponse> mResponse = SettableFuture.create();

  @Override
  public void onResponseReceived(RPCResponse response) {
    mResponse.set(response);
  }

  /**
   * Waits to receive the response and returns the response message.
   *
   * @return The {@link RPCResponse} received from the remote server
   * @throws ExecutionException if the computation threw an exception
   * @throws InterruptedException if the current thread was interrupted while waiting
   */
  public RPCResponse get() throws ExecutionException, InterruptedException {
    return mResponse.get();
  }

  /**
   * Waits to receive the response for at most a specified time, and returns the response message.
   *
   * @param timeout the maximum amount of time to wait
   * @param unit the {@link TimeUnit} of the timeout parameter
   * @return The {@link RPCResponse} received from the remote server
   * @throws ExecutionException if the computation threw an exception
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws TimeoutException if the wait timed out
   */
  public RPCResponse get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return mResponse.get(timeout, unit);
  }
}
