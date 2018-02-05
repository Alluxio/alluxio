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
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InternalException;
import alluxio.thrift.AlluxioTException;

import org.slf4j.Logger;

import java.io.IOException;

/**
 * Utilities for handling RPC calls.
 */
public final class RpcUtils {

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown. If the
   * RPC fails, a warning or error will be logged.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an exception
   */
  public static <T> T call(Logger logger, RpcCallable<T> callable)
      throws AlluxioTException {
    return call(logger, callable, true);
  }

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param logAnyFailure whether to log whenever the RPC fails
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an exception
   */
  public static <T> T call(Logger logger, RpcCallable<T> callable, boolean logAnyFailure)
      throws AlluxioTException {
    try {
      logger.debug("Enter: {}", callable);
      T ret = callable.call();
      logger.debug("Exit (OK): {}", callable);
      return ret;
    } catch (AlluxioException e) {
      logger.debug("Exit (Error): {}", callable, e);
      if (logAnyFailure && !logger.isDebugEnabled()) {
        logger.warn("{}, Error={}", callable, e.getMessage());
      }
      throw AlluxioStatusException.fromAlluxioException(e).toThrift();
    } catch (RuntimeException e) {
      logger.error("Exit (Error): {}", callable, e);
      throw new InternalException(e).toThrift();
    }
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown. If the
   * RPC fails, a warning or error will be logged.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an exception
   */
  public static <T> T call(Logger logger, RpcCallableThrowsIOException<T> callable)
      throws AlluxioTException {
    return call(logger, callable, true);
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param logAnyFailure whether to log whenever the RPC fails
   * @param <T> the return type of the callable
   * @return the return value from calling the callable
   * @throws AlluxioTException if the callable throws an exception
   */
  public static <T> T call(Logger logger, RpcCallableThrowsIOException<T> callable,
      boolean logAnyFailure) throws AlluxioTException {
    try {
      logger.debug("Enter: {}", callable);
      T ret = callable.call();
      logger.debug("Exit (OK): {}", callable);
      return ret;
    } catch (AlluxioException e) {
      logger.debug("Exit (Error): {}", callable, e);
      if (logAnyFailure && !logger.isDebugEnabled()) {
        logger.warn("{}, Error={}", callable, e.getMessage());
      }
      throw AlluxioStatusException.fromAlluxioException(e).toThrift();
    } catch (IOException e) {
      logger.debug("Exit (Error): {}", callable, e);
      if (logAnyFailure && !logger.isDebugEnabled()) {
        logger.warn("{}, Error={}", callable, e.getMessage());
      }
      throw AlluxioStatusException.fromIOException(e).toThrift();
    } catch (RuntimeException e) {
      logger.error("Exit (Error): {}", callable, e);
      throw new InternalException(e).toThrift();
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
     */
    T call() throws AlluxioException, IOException;
  }

  /**
   * An interface representing a netty RPC callable.
   *
   * @param <T> the return type of the callable
   */
  public interface NettyRPCCallable<T> {
    /**
     * The RPC implementation.
     *
     * @return the return value from the RPC
     */
    T call() throws Exception;

    /**
     * Handles exception.
     *
     * @param throwable the exception
     */
    void exceptionCaught(Throwable throwable);
  }

  /**
   * Handles a netty RPC callable with logging.
   *
   * @param logger the logger
   * @param callable the netty RPC callable
   * @param <T> the return type
   * @return the rpc result
   */
  public static <T> T nettyRPCAndLog(Logger logger, NettyRPCCallable<T> callable) {
    logger.debug("Enter: {}", callable);
    try {
      T result = callable.call();
      logger.debug("Exit (OK): {}", callable);
      return result;
    } catch (Exception e) {
      logger.debug("Exit (Error): {}, Error={}", callable, e.getMessage());
      callable.exceptionCaught(e);
    }
    return null;
  }

  private RpcUtils() {} // prevent instantiation
}
