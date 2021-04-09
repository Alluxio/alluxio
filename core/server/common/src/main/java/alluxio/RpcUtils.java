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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for handling server RPC calls.
 *
 * There are two types of RPC calls: 1. RPCs that only throw IOException 2. Streaming RPCs
 * For each of these, if failureOk is set, non-fatal errors will only be logged at the DEBUG
 * level and failure metrics will not be recorded.
 */
public final class RpcUtils {
  private RpcUtils() {} // prevent instantiation

  private static final Logger LOG = LoggerFactory.getLogger(RpcUtils.class);

  private static List<String> sMetricList = new ArrayList<>();

  static {
    // Multiple Gauges related to GC
    // For ParNew + CMS, the gauges will be:
    // ParNew.time, ParNew.count, ConcurrentMarkSweep.time, ConcurrentMarkSweep.count
    // For PS the gauges will be:
    // PS-Scavenge.count, PS-Scavenge.time, PS-MarkSweep.count, PS-MarkSweep.time
    GarbageCollectorMetricSet gcSet = new GarbageCollectorMetricSet();
    sMetricList.addAll(gcSet.getMetrics().keySet());

    // We currently track the memory usage at the granularity of heap vs non-heap
    // A finer granularity can include parts of the heap including:
    // PS-Eden-Space, PS-Old-Gen, Code-Cache, Metaspace
    sMetricList.add("heap.used");
    sMetricList.add("non-heap.used");
    LOG.info("Tracked gauges are: {}", sMetricList);
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
    long[] startValues = null;
    Instant startTime = null;
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(methodName)).time()) {
      logger.trace("Enter: {}: {}", methodName, debugDesc);
      startValues = traceGauges();
      startTime = Instant.now();
      T res = callable.call();
      logger.trace("Exit: {}: {}", methodName, debugDesc);
      reportDifference(startValues, methodName, false, startTime);
      return res;
    } catch (AlluxioException e) {
      reportDifference(startValues, methodName, false, startTime);
      logger.debug("Exit (Error): {}: {}", methodName, debugDesc, e);
      if (!failureOk) {
        MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
        if (!logger.isDebugEnabled()) {
          logger.warn("Exit (Error): {}: {}, Error={}", methodName,
              String.format(description, args), e.getMessage());
        }
      }
      throw AlluxioStatusException.fromAlluxioException(e).toGrpcStatusException();
    } catch (IOException e) {
      reportDifference(startValues, methodName, false, startTime);
      logger.debug("Exit (Error): {}: {}", methodName, debugDesc, e);
      if (!failureOk) {
        MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
        if (!logger.isDebugEnabled()) {
          logger.warn("Exit (Error): {}: {}, Error={}", methodName,
              String.format(description, args), e);
        }
      }
      throw AlluxioStatusException.fromIOException(e).toGrpcStatusException();
    } catch (RuntimeException e) {
      reportDifference(startValues, methodName, false, startTime);
      logger.error("Exit (Error): {}: {}", methodName, String.format(description, args), e);
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      throw new InternalException(e).toGrpcStatusException();
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
    long[] startValues = null;
    Instant startTime = null;
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(methodName)).time()) {
      logger.trace("Enter(stream): {}: {}", methodName, debugDesc);
      startValues = traceGauges();
      startTime = Instant.now();
      T result = callable.call();
      logger.trace("Exit(stream) (OK): {}: {}", methodName, debugDesc);
      if (sendResponse) {
        logger.trace("OnNext(stream): {}: {}", methodName, debugDesc);
        responseObserver.onNext(result);
      }
      if (completeResponse) {
        logger.trace("Completing(stream): {}: {}", methodName, debugDesc);
        responseObserver.onCompleted();
        logger.trace("Completed(stream): {}: {}", methodName, debugDesc);
      }
      reportDifference(startValues, methodName, true, startTime);
    } catch (Exception e) {
      reportDifference(startValues, methodName, true, startTime);
      logger.warn("Exit(stream) (Error): {}: {}, Error={}", methodName,
          String.format(description, args), e);
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      callable.exceptionCaught(e);
    }
  }

  private static long getGaugeValue(String name) {
    com.codahale.metrics.Metric m = MetricsSystem.METRIC_REGISTRY.getMetrics().get(name);
    return (Long) ((Gauge) m).getValue();
  }

  private static long[] traceGauges() {
    long[] numbers = new long[sMetricList.size()];
    for (int i = 0; i < sMetricList.size(); i++) {
      numbers[i] = getGaugeValue(sMetricList.get(i));
    }
    return numbers;
  }

  private static void reportDifference(long[] startValues, String rpcName, boolean isStream, Instant startTime) {
    if (startValues == null || startTime == null) {
      LOG.warn("Thread {} {}RPC {} - null in startValue={} startTime={}", Thread.currentThread().getId(),
              isStream ? "stream " : "",
              rpcName, startValues, startTime);
      return;
    }
    Instant endTime = Instant.now();
    long[] numbers = traceGauges();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numbers.length; i++) {
      sb.append(String.format("%s: %s, ", sMetricList.get(i), numbers[i] - startValues[i]));
    }
    LOG.debug("Thread {} - Time: {}, Diff in {}RPC {}: [{}]", Thread.currentThread().getId(),
            Duration.between(startTime, endTime).toMillis(),
            isStream ? "stream " : "",
            rpcName, sb.toString());
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
}
