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

package alluxio.underfs.tos;

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;

import com.volcengine.tos.TosException;
import io.grpc.Status;

import java.net.HttpURLConnection;

/**
 * Alluxio exception for tos.
 */
public class AlluxioTosException extends AlluxioRuntimeException {
  private static final ErrorType ERROR_TYPE = ErrorType.External;

  /**
   * Converts a TosClientException to a corresponding AlluxioTosException.
   *
   * @param cause tos exception
   * @return alluxio tos exception
   */
  public static AlluxioTosException from(TosException cause) { return from(null, cause); }

  /**
   * Converts a TosClientException with errormessage to a corresponding AlluxioTosException.
   *
   * @param errorMessage error message
   * @param cause tos exception
   * @return alluxio tos exception
   */
  public static AlluxioTosException from(String errorMessage, TosException cause) {
    Status status = httpStatusToGrpcStatus(cause.getStatusCode());
    String errorDescription = cause.getCode() + ":" + cause.getMessage();
    if (errorMessage == null) {
      errorMessage = errorDescription;
    }
    return new AlluxioTosException(status, errorMessage, cause, true);
  }

  private AlluxioTosException(Status status, String message, Throwable cause, boolean isRetryAble) {
    super(status, message, cause, ERROR_TYPE, isRetryAble);
  }

  private static Status httpStatusToGrpcStatus(int httpStatusCode) {
    if (httpStatusCode >= 100 && httpStatusCode < 200) {
      // 1xx. These headers should have been ignored.
      return Status.INTERNAL;
    }
    switch (httpStatusCode) {
      case HttpURLConnection.HTTP_BAD_REQUEST:  // 400
        return Status.INVALID_ARGUMENT;
      case HttpURLConnection.HTTP_UNAUTHORIZED:  // 401
        return Status.UNAUTHENTICATED;
      case HttpURLConnection.HTTP_FORBIDDEN:  // 403
        return Status.PERMISSION_DENIED;
      case HttpURLConnection.HTTP_NOT_FOUND:  // 404
        return Status.NOT_FOUND;
      case HttpURLConnection.HTTP_BAD_METHOD: // 405
      case HttpURLConnection.HTTP_NOT_IMPLEMENTED: // 501
        return Status.UNIMPLEMENTED;
      case HttpURLConnection.HTTP_CONFLICT:  // 409
        return Status.ABORTED;
      case HttpURLConnection.HTTP_LENGTH_REQUIRED: // 411
      case HttpURLConnection.HTTP_PRECON_FAILED: // 412
        return Status.FAILED_PRECONDITION;
      case 416: // Requested Range Not Satisfiable
        return Status.OUT_OF_RANGE;
      case HttpURLConnection.HTTP_INTERNAL_ERROR: //500
        return Status.INTERNAL;
      case HttpURLConnection.HTTP_MOVED_PERM:  // 301
      case HttpURLConnection.HTTP_NOT_MODIFIED: //304
      case 307: // Moved Temporarily
      case HttpURLConnection.HTTP_BAD_GATEWAY:  // 502
      case HttpURLConnection.HTTP_UNAVAILABLE:  // 503
        return Status.UNAVAILABLE;
      case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:  // 504
        return Status.DEADLINE_EXCEEDED;
      default:
        return Status.UNKNOWN;
    }
  }
}
