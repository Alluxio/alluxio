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

package alluxio.underfs.oss;

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;

import com.aliyun.oss.ServiceException;
import io.grpc.Status;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Alluxio exception for oss.
 */
public class AlluxioOSSException extends AlluxioRuntimeException {
  private static final ErrorType ERROR_TYPE = ErrorType.External;
  private static final String[] INVALID_ARGUMENT_ERROR_CODE =
      {"EntityTooLarge", "EntityTooSmall", "FileGroupTooLarge", "FilePartNotExist", "FilePartStale",
          "InvalidArgument", "InvalidBucketName", "InvalidDigest", "InvalidObjectName",
          "InvalidPart", "InvalidPartOrder", "InvalidTargetBucketForLogging", "MalformedXML",
          "RequestTimeout", "InvalidEncryptionAlgorithmError"};
  private static final Set<String> INVALID_ARGUMENT_ERROR_CODE_SET =
      new HashSet<>(Arrays.asList(INVALID_ARGUMENT_ERROR_CODE));

  /**
   * Converts an OSS ServiceException to a corresponding AlluxioOSSException.
   * @param cause oss exception
   * @return alluxio oss exception
   */
  public static AlluxioOSSException from(ServiceException cause) {
    return from(null, cause);
  }

  /**
   * Converts an OSS ServiceException with errormessage to a corresponding AlluxioOSSException.
   * @param errorMessage error message
   * @param cause oss exception
   * @return alluxio oss exception
   */
  public static AlluxioOSSException from(String errorMessage, ServiceException cause) {
    Status status = httpStatusToGrpcStatus(cause.getErrorCode());
    String errorDescription = cause.getErrorCode() + ":" + cause.getErrorMessage();
    if (errorMessage == null) {
      errorMessage = errorDescription;
    }
    return new AlluxioOSSException(status, errorMessage, cause, false);
  }

  private AlluxioOSSException(Status status, String message, Throwable cause, boolean isRetryAble) {
    super(status, message, cause, ERROR_TYPE, isRetryAble);
  }

  private static Status httpStatusToGrpcStatus(String errorCode) {
    if (INVALID_ARGUMENT_ERROR_CODE_SET.contains(errorCode)) {
      // http status code is 400
      return Status.INVALID_ARGUMENT;
    }
    if (StringUtils.isBlank(errorCode)) {
      return Status.UNKNOWN;
    }
    switch (errorCode) {
      case "InvalidAccessKeyId":  // 403
      case "RequestTimeTooSkewed": // 403
      case "SignatureDoesNotMatch": // 403
        return Status.UNAUTHENTICATED;
      case "AccessDenied":  // 403
        return Status.PERMISSION_DENIED;
      case "NoSuchBucket":  // 404
      case "NoSuchKey":  // 404
      case "NoSuchUpload":  // 404
        return Status.NOT_FOUND;
      case "MethodNotAllowed": // 405
      case "NotImplemented": // 501
        return Status.UNIMPLEMENTED;
      case "BucketAlreadyExists":  // 409
      case "BucketNotEmpty": //409
        return Status.ABORTED;
      case "MissingArgument": // 411
      case "MissingContentLength": //411
      case "PreconditionFailed": // 412
        return Status.FAILED_PRECONDITION;
      case "InternalError": //500
        return Status.INTERNAL;
      default:
        return Status.UNKNOWN;
    }
  }
}
