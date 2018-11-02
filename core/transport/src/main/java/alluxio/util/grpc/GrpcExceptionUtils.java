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

package alluxio.util.grpc;

import alluxio.exception.status.AlluxioStatusException;

import io.grpc.Status;
import io.grpc.StatusException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for conversion between alluxio and grpc status exceptions.
 */
@ThreadSafe
public final class GrpcExceptionUtils {

  private GrpcExceptionUtils() {} // prevent instantiation

  /**
   * Convert from grpc exception to alluxio status exception.
   *
   * @param e the grpc exception
   * @return the alluxio status exception
   */
  public static AlluxioStatusException fromGrpcStatusException(Exception e) {
    alluxio.exception.status.Status alluxioStatus;
    switch (Status.fromThrowable(e).getCode()) {
      case ABORTED:
        alluxioStatus = alluxio.exception.status.Status.ABORTED;
        break;
      case ALREADY_EXISTS:
        alluxioStatus = alluxio.exception.status.Status.ALREADY_EXISTS;
        break;
      case CANCELLED:
        alluxioStatus = alluxio.exception.status.Status.CANCELED;
        break;
      case DATA_LOSS:
        alluxioStatus = alluxio.exception.status.Status.DATA_LOSS;
        break;
      case DEADLINE_EXCEEDED:
        alluxioStatus = alluxio.exception.status.Status.DEADLINE_EXCEEDED;
        break;
      case FAILED_PRECONDITION:
        alluxioStatus = alluxio.exception.status.Status.FAILED_PRECONDITION;
        break;
      case INTERNAL:
        alluxioStatus = alluxio.exception.status.Status.INTERNAL;
        break;
      case INVALID_ARGUMENT:
        alluxioStatus = alluxio.exception.status.Status.INVALID_ARGUMENT;
        break;
      case NOT_FOUND:
        alluxioStatus = alluxio.exception.status.Status.NOT_FOUND;
        break;
      case OK:
        alluxioStatus = alluxio.exception.status.Status.OK;
        break;
      case OUT_OF_RANGE:
        alluxioStatus = alluxio.exception.status.Status.OUT_OF_RANGE;
        break;
      case PERMISSION_DENIED:
        alluxioStatus = alluxio.exception.status.Status.PERMISSION_DENIED;
        break;
      case RESOURCE_EXHAUSTED:
        alluxioStatus = alluxio.exception.status.Status.RESOURCE_EXHAUSTED;
        break;
      case UNAUTHENTICATED:
        alluxioStatus = alluxio.exception.status.Status.UNAUTHENTICATED;
        break;
      case UNAVAILABLE:
        alluxioStatus = alluxio.exception.status.Status.UNAVAILABLE;
        break;
      case UNIMPLEMENTED:
        alluxioStatus = alluxio.exception.status.Status.UNIMPLEMENTED;
        break;
      case UNKNOWN:
        alluxioStatus = alluxio.exception.status.Status.UNKNOWN;
        break;
      default:
        alluxioStatus = alluxio.exception.status.Status.UNKNOWN;
    }
    String message = (e.getCause() == null || e.getCause().getMessage() == null) ? e.getMessage()
        : e.getCause().getMessage();
    return AlluxioStatusException.from(alluxioStatus, message);
  }

  /**
   * Convert from alluxio status exception to grpc exception.
   *
   * @param e the alluxio status exception
   * @return the grpc status exception
   */
  public static StatusException toGrpcStatusException(AlluxioStatusException e) {
    Status.Code code;
    switch (e.getStatus()) {
      case ABORTED:
        code = Status.Code.ABORTED;
        break;
      case ALREADY_EXISTS:
        code = Status.Code.ALREADY_EXISTS;
        break;
      case CANCELED:
        code = Status.Code.CANCELLED;
        break;
      case DATA_LOSS:
        code = Status.Code.DATA_LOSS;
        break;
      case DEADLINE_EXCEEDED:
        code = Status.Code.DEADLINE_EXCEEDED;
        break;
      case FAILED_PRECONDITION:
        code = Status.Code.FAILED_PRECONDITION;
        break;
      case INTERNAL:
        code = Status.Code.INTERNAL;
        break;
      case INVALID_ARGUMENT:
        code = Status.Code.INVALID_ARGUMENT;
        break;
      case NOT_FOUND:
        code = Status.Code.NOT_FOUND;
        break;
      case OK:
        code = Status.Code.OK;
        break;
      case OUT_OF_RANGE:
        code = Status.Code.OUT_OF_RANGE;
        break;
      case PERMISSION_DENIED:
        code = Status.Code.PERMISSION_DENIED;
        break;
      case RESOURCE_EXHAUSTED:
        code = Status.Code.RESOURCE_EXHAUSTED;
        break;
      case UNAUTHENTICATED:
        code = Status.Code.UNAUTHENTICATED;
        break;
      case UNAVAILABLE:
        code = Status.Code.UNAVAILABLE;
        break;
      case UNIMPLEMENTED:
        code = Status.Code.UNIMPLEMENTED;
        break;
      case UNKNOWN:
        code = Status.Code.UNKNOWN;
        break;
      default:
        code = Status.Code.UNKNOWN;
    }
    return Status.fromCode(code).withCause(e).asException();
  }
}

