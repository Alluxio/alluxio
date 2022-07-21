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

package alluxio.underfs.s3a;

import alluxio.exception.AlluxioRuntimeException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.protobuf.Any;
import io.grpc.Status;

import java.net.HttpURLConnection;

/**
 * Alluxio exception for s3.
 */
public class AlluxioS3Exception extends AlluxioRuntimeException {

  /**
   * Converts an AmazonClientException to a corresponding AlluxioS3Exception.
   * @param cause aws s3 exception
   * @return alluxio s3 exception
   */
  public static AlluxioS3Exception from(AmazonClientException cause) {
    return fromWithErrorMessage(null, cause);
  }

  /**
   * Converts an AmazonClientException with errormessage to a corresponding AlluxioS3Exception.
   * @param errorMessage error message
   * @param cause aws s3 exception
   * @return alluxio s3 exception
   */
  public static AlluxioS3Exception fromWithErrorMessage(String errorMessage,
                                                        AmazonClientException cause) {
    Status status = Status.UNKNOWN;
    String errorDescription = "ClientException:" + cause.getMessage();
    if (cause instanceof AmazonS3Exception) {
      AmazonS3Exception exception = (AmazonS3Exception) cause;
      status = httpStatusToGrpcStatus(exception.getStatusCode());
      errorDescription = exception.getErrorCode() + ":" + exception.getErrorMessage();
    }
    if (errorMessage == null) {
      errorMessage = errorDescription;
    }
    return new AlluxioS3Exception(status, errorMessage, cause, cause.isRetryable());
  }

  private AlluxioS3Exception(Status status, String message, Throwable cause, boolean isRetryAble,
      Any... details) {
    super(status, message, cause, isRetryAble, details);
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
      case 411: // Content-Length HTTP header required
      case 412: // Precondition Failed
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
