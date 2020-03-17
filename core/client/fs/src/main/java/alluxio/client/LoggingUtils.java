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

package alluxio.client;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;

import java.io.IOException;

/**
 * Utils for client-side logging.
 */
public final  class LoggingUtils {
  private static final long THRESHOLD = new InstancedConfiguration(ConfigurationUtils.defaults())
      .getMs(PropertyKey.USER_LOGGING_THRESHOLD);

  /**
   * The method to be executed.
   *
   * @param <V> the return value of {@link #call()}
   */
  public interface Callable<V> {
    /**
     * Execute the task.
     *
     * @return result
     * @throws IOException when any exception defined in gRPC happens
     */
    V call() throws IOException;
  }

  /**
   * Execute a method and log the method in debug level.
   *
   * @param <V> type of return value of the RPC call
   * @param method the method to be executed
   * @param logger the logger to use for this call
   * @param methodName the human readable name of the RPC call
   * @param description the format string of the description, used for logging
   * @param args the arguments for the description
   * @return the return value of the RPC call
   * @throws IOException
   */
  public static <V> V callAndLog(Callable<V> method, Logger logger, String methodName,
      String description, Object... args) throws IOException {
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    long tid = Thread.currentThread().getId();
    long startMs = System.currentTimeMillis();
    logger.debug("Enter: {}({}), tid={}", methodName, debugDesc, tid);
    try {
      V ret = method.call();
      long duration = System.currentTimeMillis() - startMs;
      logger.debug("Exit (OK): {}({}) in {} ms, tid={}", methodName, debugDesc, duration, tid);
      if (duration >= THRESHOLD) {
        logger.warn("{}({}) returned {} in {} ms (>={}ms), tid={}",
            methodName, String.format(description, args), ret, duration, THRESHOLD, tid);
      }
      return ret;
    } catch (Exception e) {
      long duration = System.currentTimeMillis() - startMs;
      logger.debug("Exit (ERROR): {}({}) in {} ms: {}, tid={}", methodName, debugDesc, duration,
          e.toString(), tid);
      if (duration >= THRESHOLD) {
        logger.warn("{}({}) failed with exception [{}] in {} ms (>={}ms), tid={}", methodName,
            String.format(description, args), e.toString(), duration, THRESHOLD, tid);
      }
      throw e;
    }
  }

  private LoggingUtils() {} // prevent initialization
}
