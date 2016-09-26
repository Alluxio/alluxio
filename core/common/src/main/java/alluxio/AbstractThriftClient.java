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

package alluxio;

import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.ThriftIOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for clients that use {@link alluxio.network.connection.ThriftClientPool}.
 */
// TODO(peis): Rename this to Client once we have deprecated AbstractClient and Client class.
@ThreadSafe
public abstract class AbstractThriftClient {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int RPC_MAX_NUM_RETRY = 30;

  /**
   * @return a Thrift service client
   */
  protected abstract AlluxioService.Client acquireClient() throws IOException;

  /**
   * @param client the client to release
   */
  protected abstract void releaseClient(AlluxioService.Client client);

  /**
   * The RPC to be executed in {@link #retryRPC(RpcCallable)}.
   *
   * @param <V> the return value of {@link #call(AlluxioService.Client)}
   */
  protected interface RpcCallable<V> {
    /**
     * The task where RPC happens.
     *
     * @return RPC result
     * @throws TException when any exception defined in thrift happens
     */
    V call(AlluxioService.Client client) throws TException;
  }

  /**
   * Same with {@link RpcCallable} except that this RPC call throws {@link AlluxioTException} and
   * is to be executed in {@link #retryRPC(RpcCallableThrowsAlluxioTException)}.
   *
   * @param <V> the return value of {@link #call(AlluxioService.Client)}
   */
  protected interface RpcCallableThrowsAlluxioTException<V> {
    /**
     * The task where RPC happens.
     *
     * @return RPC result
     * @throws AlluxioTException when any {@link AlluxioException} happens during RPC and is wrapped
     *         into {@link AlluxioTException}
     * @throws TException when any exception defined in thrift happens
     */
    V call(AlluxioService.Client client) throws AlluxioTException, TException;
  }

  /**
   * Tries to execute an RPC defined as a {@link RpcCallable}.
   *
   * @param rpc the RPC call to be executed
   * @param <V> type of return value of the RPC call
   * @return the return value of the RPC call
   * @throws IOException when retries exceeds {@link #RPC_MAX_NUM_RETRY} or some server
   *         side IOException occurred.
   */
  protected <V> V retryRPC(RpcCallable<V> rpc) throws IOException {
    int countingRetry = 0;
    TException exception = null;
    while ((countingRetry++) <= RPC_MAX_NUM_RETRY) {
      AlluxioService.Client client = acquireClient();
      try {
        return rpc.call(client);
      } catch (ThriftIOException e) {
        throw new IOException(e);
      } catch (AlluxioTException e) {
        throw Throwables.propagate(AlluxioException.fromThrift(e));
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        closeClient(client);
        exception = e;
      } finally {
        releaseClient(client);
      }
    }

    LOG.error("Failed after " + countingRetry + " retries.");
    Preconditions.checkNotNull(exception);
    throw new IOException(exception);
  }

  /**
   * Similar to {@link #retryRPC(RpcCallable)} except that the RPC call may throw
   * {@link AlluxioTException} and once it is thrown, it will be transformed into
   * {@link AlluxioException} and be thrown.
   *
   * @param rpc the RPC call to be executed
   * @param <V> type of return value of the RPC call
   * @return the return value of the RPC call
   * @throws AlluxioException when {@link AlluxioTException} is thrown by the RPC call
   * @throws IOException when retries exceeds {@link #RPC_MAX_NUM_RETRY} or some server
   *         side IOException occurred.
   */
  protected <V> V retryRPC(RpcCallableThrowsAlluxioTException<V> rpc)
      throws AlluxioException, IOException {
    int countingRetry = 0;
    TException exception = null;
    while ((countingRetry++) <= RPC_MAX_NUM_RETRY) {
      AlluxioService.Client client = acquireClient();
      try {
        return rpc.call(client);
      } catch (AlluxioTException e) {
        throw AlluxioException.fromThrift(e);
      } catch (ThriftIOException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        closeClient(client);
        exception = e;
      } finally {
        releaseClient(client);
      }
    }

    LOG.error("Failed after " + countingRetry + " retries.");
    Preconditions.checkNotNull(exception);
    throw new IOException(exception);
  }

  /**
   * Close the given client.
   *
   * @param client the client to close
   */
  private void closeClient(AlluxioService.Client client) {
    client.getOutputProtocol().getTransport().close();
  }
}
