/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.netty;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.SettableFuture;

import tachyon.Constants;
import tachyon.network.protocol.RPCResponse;

/**
 * A simple listener that waits for a single {@link RPCResponse} message from the remote server.
 */
public final class SingleResponseListener implements ClientHandler.ResponseListener {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private SettableFuture<RPCResponse> mResponse = SettableFuture.create();

  @Override
  public void onResponseReceived(RPCResponse response) {
    mResponse.set(response);
  }

  /**
   * Waits to receive the response and returns the response message.
   *
   * @return The {@link RPCResponse} received from the remote server.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public RPCResponse get() throws ExecutionException, InterruptedException {
    return mResponse.get();
  }

  /**
   * Waits to receive the response for at most a specified time, and returns the response message.
   *
   * @param timeout the maximum amount of time to wait.
   * @param unit the {@link TimeUnit} of the timeout parameter.
   * @return The {@link RPCResponse} received from the remote server.
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  public RPCResponse get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return mResponse.get(timeout, unit);
  }
}
