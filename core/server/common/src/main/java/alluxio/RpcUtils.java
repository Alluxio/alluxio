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
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.grpc.GrpcExceptionUtils;

import com.codahale.metrics.Timer;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * Utilities for handling server RPC calls.
 *
 * There are three types of RPC calls: 1. RPCs that only throw AlluxioException 2. RPCs that throw
 * AlluxioException and IOException 3. Netty RPCs
 *
 * For each of these, there are two types of methods 1. call(callable) - for internal methods,
 * executes the method without any logging or metrics 2. call(logger, callable, method name,
 * failureOk, method description, arguments...) - for client initiated methods, executes the method
 * with logging and metrics. If failureOk is set, non-fatal errors will only be logged at the DEBUG
 * level and failure metrics will not be recorded.
 */
public final class RpcUtils {
  private RpcUtils() {} // prevent instantiation

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown. No call history or
   * errors will be logged. This method should be used for internal RPCs.
   *
   * @param callable the callable to call
   * @param <T> the return type of the callable
   * @param responseObserver gRPC response observer
   */
  public static <T> void call(RpcCallable<T> callable, StreamObserver<T> responseObserver) {
    try {
      T ret = callable.call();
      responseObserver.onNext(ret);
      responseObserver.onCompleted();
    } catch (AlluxioException e) {
      responseObserver.onError(
          GrpcExceptionUtils.toGrpcStatusException(AlluxioStatusException.fromAlluxioException(e)));
    } catch (RuntimeException e) {
      responseObserver.onError(GrpcExceptionUtils.toGrpcStatusException(new InternalException(e)));
    }
  }

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown. If the RPC fails, a
   * warning or error will be logged.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param description the format string of the description, used for logging
   * @param responseObserver gRPC response observer
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   */
  public static <T> void call(Logger logger, RpcCallable<T> callable, String methodName,
      String description, StreamObserver<T> responseObserver, Object... args) {
    call(logger, callable, methodName, false, description, responseObserver, args);
  }

  /**
   * Calls the given {@link RpcCallable} and handles any exceptions thrown.
   *
   * The failureOk parameter indicates whether or not AlluxioExceptions are expected results (for
   * example it would be false for the exists() call). In this case, we do not log the failure or
   * increment failure metrics. When a RuntimeException is thrown, we always treat it as a failure
   * and log an error and increment metrics.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param failureOk whether failures are expected (affects logging and metrics)
   * @param description the format string of the description, used for logging
   * @param responseObserver gRPC response observer
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   */
  public static <T> void call(Logger logger, RpcCallable<T> callable, String methodName,
      boolean failureOk, String description, StreamObserver<T> responseObserver, Object... args) {
    // avoid string format for better performance if debug is off
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(methodName)).time()) {
      logger.debug("Enter: {}: {}", methodName, debugDesc);
      T ret = callable.call();
      logger.debug("Exit (OK): {}: {}", methodName, debugDesc);
      responseObserver.onNext(ret);
      responseObserver.onCompleted();
    } catch (AlluxioException e) {
      logger.debug("Exit (Error): {}: {}", methodName, debugDesc, e);
      if (!failureOk) {
        MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
        if (!logger.isDebugEnabled()) {
          logger.warn("Exit (Error): {}: {}, Error={}", methodName,
              String.format(description, args), e);
        }
      }
      responseObserver.onError(
          GrpcExceptionUtils.toGrpcStatusException(AlluxioStatusException.fromAlluxioException(e)));
    } catch (RuntimeException e) {
      logger.error("Exit (Error): {}: {}", methodName, String.format(description, args), e);
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      responseObserver.onError(GrpcExceptionUtils.toGrpcStatusException(new InternalException(e)));
    }
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown. No call
   * history or errors will be logged. This method should be used for internal RPCs.
   *
   * @param callable the callable to call
   * @param responseObserver gRPC response observer
   * @param <T> the return type of the callable
   */
  public static <T> void call(RpcCallableThrowsIOException<T> callable,
      StreamObserver<T> responseObserver) {
    try {
      T ret = callable.call();
      responseObserver.onNext(ret);
      responseObserver.onCompleted();
    } catch (AlluxioException e) {
      responseObserver.onError(
          GrpcExceptionUtils.toGrpcStatusException(AlluxioStatusException.fromAlluxioException(e)));
    } catch (IOException e) {
      responseObserver.onError(
          GrpcExceptionUtils.toGrpcStatusException(AlluxioStatusException.fromIOException(e)));
    } catch (RuntimeException e) {
      responseObserver.onError(GrpcExceptionUtils.toGrpcStatusException(new InternalException(e)));
    }
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown. If the
   * RPC fails, a warning or error will be logged.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param description the format string of the description, used for logging
   * @param responseObserver gRPC response observer
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   */
  public static <T> void call(Logger logger, RpcCallableThrowsIOException<T> callable,
      String methodName, String description, StreamObserver<T> responseObserver, Object... args) {
    call(logger, callable, methodName, false, description, responseObserver, args);
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown.
   *
   * The failureOk parameter indicates whether or not AlluxioExceptions and IOExceptions are
   * expected results (for example it would be false for the exists() call). In this case, we do not
   * log the failure or increment failure metrics. When a RuntimeException is thrown, we always
   * treat it as a failure and log an error and increment metrics.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param failureOk whether failures are expected (affects logging and metrics)
   * @param description the format string of the description, used for logging
   * @param responseObserver gRPC response observer
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   */
  public static <T> void call(Logger logger, RpcCallableThrowsIOException<T> callable,
      String methodName, boolean failureOk, String description, StreamObserver<T> responseObserver,
      Object... args) {
    // avoid string format for better performance if debug is off
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(methodName)).time()) {
      logger.debug("Enter: {}: {}", methodName, debugDesc);
      T ret = callable.call();
      logger.debug("Exit (OK): {}: {}", methodName, debugDesc);
      responseObserver.onNext(ret);
      responseObserver.onCompleted();
    } catch (AlluxioException e) {
      logger.debug("Exit (Error): {}: {}", methodName, debugDesc, e);
      if (!failureOk) {
        MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
        if (!logger.isDebugEnabled()) {
          logger.warn("Exit (Error): {}: {}, Error={}", methodName,
              String.format(description, args), e);
        }
      }
      responseObserver.onError(
          GrpcExceptionUtils.toGrpcStatusException(AlluxioStatusException.fromAlluxioException(e)));
    } catch (IOException e) {
      logger.debug("Exit (Error): {}: {}", methodName, debugDesc, e);
      if (!failureOk) {
        MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
        if (!logger.isDebugEnabled()) {
          logger.warn("Exit (Error): {}: {}, Error={}", methodName,
              String.format(description, args), e);
        }
      }
      responseObserver.onError(
          GrpcExceptionUtils.toGrpcStatusException(AlluxioStatusException.fromIOException(e)));
    } catch (RuntimeException e) {
      logger.error("Exit (Error): {}: {}", methodName, String.format(description, args), e);
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      responseObserver.onError(GrpcExceptionUtils.toGrpcStatusException(new InternalException(e)));
    }
  }

  /**
   * Handles a netty RPC callable with logging.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param description the format string of the description, used for logging
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   * @return the rpc result
   */
  public static <T> T nettyRPCAndLog(Logger logger, NettyRpcCallable<T> callable, String methodName,
      String description, Object... args) {
    // avoid string format for better performance if debug is off
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(methodName)).time()) {
      logger.debug("Enter: {}: {}", methodName, debugDesc);
      T result = callable.call();
      logger.debug("Exit (OK): {}: {}", methodName, debugDesc);
      return result;
    } catch (Exception e) {
      logger.warn("Exit (Error): {}: {}, Error={}", methodName, String.format(description, args),
          e);
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      callable.exceptionCaught(e);
    }
    return null;
  }

  /**
   * Handles a netty RPC callable with logging.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param description the format string of the description, used for logging
   * @param responseObserver gRPC response observer
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   */
  public static <T> void nettyRPCAndLog(Logger logger, NettyRpcCallable<T> callable,
      String methodName, String description, StreamObserver<T> responseObserver, Object... args) {
    // avoid string format for better performance if debug is off
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(methodName)).time()) {
      logger.debug("Enter: {}: {}", methodName, debugDesc);
      T result = callable.call();
      logger.debug("Exit (OK): {}: {}", methodName, debugDesc);
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.warn("Exit (Error): {}: {}, Error={}", methodName, String.format(description, args),
          e);
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      callable.exceptionCaught(e);
    }
  }

  private static String getQualifiedMetricName(String methodName) {
    return getQualifiedMetricNameInternal(methodName);
  }

  private static String getQualifiedFailureMetricName(String methodName) {
    return getQualifiedMetricNameInternal(methodName + "Failures");
  }

  private static String getQualifiedMetricNameInternal(String name) {
    User user = AuthenticatedClientUser.getOrNull();
    if (user != null) {
      return Metric.getMetricNameWithUserTag(name, user.getName());
    }
    return name;
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
  public interface NettyRpcCallable<T> {
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
}
