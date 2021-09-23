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

import alluxio.conf.SensitiveConfigMask;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InternalException;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;

import com.codahale.metrics.Timer;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * Utilities for handling server RPC calls.
 *
 * There are two types of RPC calls: 1. RPCs that only throw IOException 2. Streaming RPCs
 * For each of these, if failureOk is set, non-fatal errors will only be logged at the DEBUG
 * level and failure metrics will not be recorded.
 */
public final class RpcUtils {
  private RpcUtils() {} // prevent instantiation

  public static final SensitiveConfigMask SENSITIVE_CONFIG_MASKER =
      RpcSensitiveConfigMask.CREDENTIAL_FIELD_MASKER;

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
    T response;
    try {
      response = callAndReturn(logger, callable, methodName, failureOk, description, args);
    } catch (StatusException e) {
      responseObserver.onError(e);
      return;
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /**
   *
   * Calls the given {@link RpcCallableThrowsIOException} and returns its result. Exceptions are
   * logged, accounted for in metrics and then rethrown at the end.
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
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   * @return the result. (Null if failed)
   */
  public static <T> T callAndReturn(Logger logger, RpcCallableThrowsIOException<T> callable,
      String methodName, boolean failureOk, String description, Object... args)
      throws StatusException {
    // avoid string format for better performance if debug is off
    String debugDesc = logger.isDebugEnabled() ? String.format(description,
        processObjects(logger, args)) : null;
    try (MetricsSystem.MultiTimerContext ctx = new MetricsSystem.MultiTimerContext(
        Metrics.TOTAL_RPCS, MetricsSystem.timer(getQualifiedMetricName(methodName)))) {
      MetricsSystem.counter(getQualifiedInProgressMetricName(methodName)).inc();
      logger.debug("Enter: {}: {}", methodName, debugDesc);
      T res = callable.call();
      logger.debug("Exit: {}: {}", methodName, debugDesc);
      return res;
    } catch (AlluxioException e) {
      logger.debug("Exit (Error): {}: {}", methodName, debugDesc, e);
      if (!failureOk) {
        MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
        if (!logger.isDebugEnabled()) {
          logger.warn("Exit (Error): {}: {}, Error={}", methodName,
              String.format(description, processObjects(logger, args)),
              e.toString());
        }
      }
      throw AlluxioStatusException.fromAlluxioException(e).toGrpcStatusException();
    } catch (IOException e) {
      logger.debug("Exit (Error): {}: {}", methodName, debugDesc, e);
      if (!failureOk) {
        MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
        if (!logger.isDebugEnabled()) {
          logger.warn("Exit (Error): {}: {}, Error={}", methodName,
              String.format(description, processObjects(logger, args)),
              e.toString());
        }
      }
      throw AlluxioStatusException.fromIOException(e).toGrpcStatusException();
    } catch (RuntimeException e) {
      logger.error("Exit (Error): {}: {}", methodName,
          String.format(description, processObjects(logger, args)), e);
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      throw new InternalException(e).toGrpcStatusException();
    } finally {
      MetricsSystem.counter(getQualifiedInProgressMetricName(methodName)).dec();
    }
  }

  /**
   * Handles a streaming RPC callable with logging.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param sendResponse whether a response should send to the client
   * @param completeResponse whether onComplete should be called on the response observer
   * @param responseObserver gRPC response observer
   * @param description the format string of the description, used for logging
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   */
  public static <T> void streamingRPCAndLog(Logger logger, StreamingRpcCallable<T> callable,
      String methodName, boolean sendResponse, boolean completeResponse,
      StreamObserver<T> responseObserver, String description, Object... args) {
    // avoid string format for better performance if debug is off
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(methodName)).time()) {
      MetricsSystem.counter(getQualifiedInProgressMetricName(methodName)).inc();
      logger.debug("Enter(stream): {}: {}", methodName, debugDesc);
      T result = callable.call();
      logger.debug("Exit(stream) (OK): {}: {}", methodName, debugDesc);
      if (sendResponse) {
        logger.debug("OnNext(stream): {}: {}", methodName, debugDesc);
        responseObserver.onNext(result);
      }
      if (completeResponse) {
        logger.debug("Completing(stream): {}: {}", methodName, debugDesc);
        responseObserver.onCompleted();
        logger.debug("Completed(stream): {}: {}", methodName, debugDesc);
      }
    } catch (Exception e) {
      logger.warn("Exit(stream) (Error): {}: {}, Error={}", methodName,
          String.format(description, args), e.toString());
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      callable.exceptionCaught(e);
    } finally {
      MetricsSystem.counter(getQualifiedInProgressMetricName(methodName)).dec();
    }
  }

  protected static Object[] processObjects(Logger logger, Object... args) {
    return SENSITIVE_CONFIG_MASKER == null
        ? args : SENSITIVE_CONFIG_MASKER.maskObjects(logger, args);
  }

  private static String getQualifiedMetricName(String methodName) {
    return getQualifiedMetricNameInternal(methodName);
  }

  private static String getQualifiedFailureMetricName(String methodName) {
    return getQualifiedMetricNameInternal(methodName , "Failures");
  }

  private static String getQualifiedInProgressMetricName(String methodName) {
    return getQualifiedMetricNameInternal(methodName, "InProgress");
  }

  private static String getQualifiedMetricNameInternal(String ... components) {
    User user = AuthenticatedClientUser.getOrNull();
    if (user != null) {
      return Metric.getMetricNameWithUserTag(String.join("", components), user.getName());
    }
    return String.join("", components);
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
   * An interface representing a streaming RPC callable.
   *
   * @param <T> the return type of the callable
   */
  public interface StreamingRpcCallable<T> {
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

  private static final class Metrics {
    /** RPC throughput. */
    private static final Timer TOTAL_RPCS =
        MetricsSystem.timer(MetricKey.MASTER_TOTAL_RPCS.getName());
  }
}
