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

package alluxio.job;

import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Utility for getting error information in job service.
 */
public class ErrorUtils {

  /**
   * @param t exception that was thrown
   * @return error type of the root cause
   */
  public static String getErrorType(Throwable t) {
    Throwable cause = ExceptionUtils.getRootCause(t);

    if (cause == null) {
      cause = t;
    }

    return cause.getClass().getSimpleName();
  }

  private ErrorUtils() {} // prevent instantiation
}
