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

package alluxio;

import alluxio.exception.AlluxioException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.ThriftIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utilities for handling RPC calls.
 */
public final class RpcUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown.
   *
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an Alluxio or runtime exception
   */
  public static <T> T call(RpcCallable<T> callable) throws AlluxioTException {
    try {
      return callable.call();
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (RuntimeException e) {
      LOG.error("Unexpected error running rpc", e);
      throw new UnexpectedAlluxioException(e).toAlluxioTException();
    }
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown.
   *
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an Alluxio or runtime exception
   * @throws ThriftIOException if the callable throws an IOException
   */
  public static <T> T call(RpcCallableThrowsIOException<T> callable)
      throws AlluxioTException, ThriftIOException {
    try {
      return callable.call();
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (RuntimeException e) {
      LOG.error("Unexpected error running rpc", e);
      throw new UnexpectedAlluxioException(e).toAlluxioTException();
    }
  }

  /**
   * An interface representing a callable which can only throw Alluxio exceptions.
   *
   * @param <T> the return type of the callable
   */
  public interface RpcCallable<T> {
    /**
     * The RPC implementation.
     *
     * @return the return value from the RPC
     * @throws AlluxioException if an expected exception occurs in the Alluxio system
     */
    T call() throws AlluxioException;
  }

  /**
   * An interface representing a callable which can only throw Alluxio or IO exceptions.
   *
   * @param <T> the return type of the callable
   */
  public interface RpcCallableThrowsIOException<T> {
    /**
     * The RPC implementation.
     *
     * @return the return value from the RPC
     * @throws AlluxioException if an expected exception occurs in the Alluxio system
     * @throws IOException if an exception is thrown when interacting with the underlying system
     */
    T call() throws AlluxioException, IOException;
  }

  private RpcUtils() {} // prevent instantiation
}
