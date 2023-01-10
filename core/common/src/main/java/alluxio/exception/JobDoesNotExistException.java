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

package alluxio.exception;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The exception thrown when a job definition does not exist in Alluxio.
 */
@ThreadSafe
public class JobDoesNotExistException extends AlluxioException {
  private static final long serialVersionUID = -7291730624984048562L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public JobDoesNotExistException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception saying the specified job ID does not exist.
   *
   * @param jobId the job id which does not exit
   */
  public JobDoesNotExistException(long jobId) {
    this(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(String.valueOf(jobId)));
  }
}
