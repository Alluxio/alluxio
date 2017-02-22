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
  private static final Logger LOG = LoggerFactory.getLogger(RpcUtils.class);

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
      LOG.debug("Internal Error: {}", callable, e);
      throw e.toThrift();
    } catch (Exception e) {
      LOG.error("Unexpected Error: {}", callable, e);
      throw new UnexpectedAlluxioException(e).toThrift();
    }
  }

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown. This method also logs
   * enter and exit when debug level logging is enabled.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an Alluxio or runtime exception
   */
  public static <T> T call(Logger logger, RpcCallable<T> callable) throws AlluxioTException {
    try {
      logger.debug("Enter: {}", callable);
      T ret = callable.call();
      logger.debug("Exit: {}", callable);
      return ret;
    } catch (AlluxioException e) {
      logger.debug("Internal Error: {}", callable, e);
      throw e.toThrift();
    } catch (Exception e) {
      logger.error("Unexpected Error: {}", callable, e);
      throw new UnexpectedAlluxioException(e).toThrift();
    }
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown. This
   * method also logs enter and exit when debug level logging is enabled.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an Alluxio or runtime exception
   * @throws ThriftIOException if the callable throws an IOException
   */
  public static <T> T call(Logger logger, RpcCallableThrowsIOException<T> callable)
      throws AlluxioTException, ThriftIOException {
    try {
      logger.debug("Enter: {}", callable);
      T ret = callable.call();
      logger.debug("Exit: {}", callable);
      return ret;
    } catch (AlluxioException e) {
      logger.debug("Internal Error: {}", callable, e);
      throw e.toThrift();
    } catch (IOException e) {
      logger.debug("I/O Error: {}", callable, e);
      throw new ThriftIOException(e.getMessage());
    } catch (Exception e) {
      logger.error("Unexpected Error: {}", callable, e);
      throw new UnexpectedAlluxioException(e).toThrift();
    }
  }

  /**
   * An interface representing a callable which can only throw Alluxio exceptions. The toString
   * method of classes implementing this interface should be in the format "CallName.
   * arg1:value1, arg2:value2, ...".
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
   * An interface representing a callable which can only throw Alluxio or IO exceptions. The
   * toString method of classes implementing this interface should be in the format "CallName.
   * arg1:value1, arg2:value2, ...".
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
