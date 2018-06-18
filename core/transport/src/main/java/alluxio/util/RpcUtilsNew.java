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

package alluxio.util;

import java.io.IOException;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InternalException;

/**
 * Utilities for handling RPC calls.
 */
public final class RpcUtilsNew {

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown. If the RPC fails, a
   * warning or error will be logged.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param <T> the return type of the callable
   */
  public static <T> void call(Logger logger, RpcCallable<T> callable,
      StreamObserver<T> streamObserver) {
    call(logger, callable, streamObserver, true);
  }

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param responseObserver where to send results of the RPC call
   * @param logAnyFailure whether to log whenever the RPC fails
   * @param <T> the return type of the callable
   */
  public static <T> void call(Logger logger, RpcCallable<T> callable,
      StreamObserver<T> responseObserver, boolean logAnyFailure) {
    try {
      logger.debug("Enter: {}", callable);
      T ret = callable.call();
      logger.debug("Exit (OK): {}", callable);
      responseObserver.onNext(ret);
      responseObserver.onCompleted();
    } catch (AlluxioException e) {
      logger.debug("Exit (Error): {}", callable, e);
      if (logAnyFailure && !logger.isDebugEnabled()) {
        logger.warn("{}, Error={}", callable, e.getMessage());
      }
      responseObserver.onError(AlluxioStatusException.fromAlluxioException(e));
    } catch (RuntimeException e) {
      logger.error("Exit (Error): {}", callable, e);
      responseObserver.onError(new InternalException(e));
    }
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown. If the
   * RPC fails, a warning or error will be logged.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param responseObserver where to send results of the RPC call
   * @param <T> the return type of the callable
   */
  public static <T> void call(Logger logger, RpcCallableThrowsIOException<T> callable,
      StreamObserver<T> responseObserver) {
    call(logger, callable, responseObserver, true);
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param responseObserver where to send results of the RPC call
   * @param logAnyFailure whether to log whenever the RPC fails
   * @param <T> the return type of the callable
   */
  public static <T> void call(Logger logger, RpcCallableThrowsIOException<T> callable,
      StreamObserver<T> responseObserver, boolean logAnyFailure) {
    try {
      logger.debug("Enter: {}", callable);
      T ret = callable.call();
      logger.debug("Exit (OK): {}", callable);
      responseObserver.onNext(ret);
      responseObserver.onCompleted();
    } catch (AlluxioException e) {
      logger.debug("Exit (Error): {}", callable, e);
      if (logAnyFailure && !logger.isDebugEnabled()) {
        logger.warn("{}, Error={}", callable, e.getMessage());
      }
      responseObserver.onError(AlluxioStatusException.fromAlluxioException(e));
    } catch (IOException e) {
      logger.debug("Exit (Error): {}", callable, e);
      if (logAnyFailure && !logger.isDebugEnabled()) {
        logger.warn("{}, Error={}", callable, e.getMessage());
      }
      responseObserver.onError(AlluxioStatusException.fromIOException(e));
    } catch (RuntimeException e) {
      logger.error("Exit (Error): {}", callable, e);
      responseObserver.onError(new InternalException(e));
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

  private RpcUtilsNew() {} // prevent instantiation
}
