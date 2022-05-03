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

import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.grpc.StatusException;

/**
 * Alluxio RuntimeException. Every developer should throw this exception when need to surface
 * exception to client.
 */
public class AlluxioRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 7801880681732804395L;
  private final Status mStatus;

  /**
   * @param status the grpc status code for this exception
   * @param message the error message
   */
  public AlluxioRuntimeException(Status status, String message) {
    this(status, message, null);
  }

  /**
   * @param status the grpc status code for this exception
   * @param cause the exception
   */
  public AlluxioRuntimeException(Status status, Throwable cause) {
    this(status, null, cause);
  }

  /**
   * @param status the grpc status code for this exception
   * @param message the error message
   * @param cause the exception
   */
  public AlluxioRuntimeException(Status status, String message, Throwable cause) {
    super(message, cause);
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(status != Status.OK, "OK is not an error status");
    mStatus = status;
  }

  /**
   * @return grpc status
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @return a gRPC status exception representation of this exception
   */
  public StatusException toGrpcStatusException() {
    return mStatus.withCause(getCause()).withDescription(getMessage()).asException();
  }

  @Override
  public String getMessage() {
    String message = super.getMessage();
    if (message == null && getCause() != null) {
      message = getCause().getMessage();
    }
    if (message == null) {
      message = mStatus.getDescription();
    }
    return message;
  }
}
