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

package alluxio.grpc;

import alluxio.exception.status.AlluxioStatusException;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang.SerializationUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

/**
 * Utility methods for conversion between alluxio and grpc status exceptions.
 */
@ThreadSafe
public final class GrpcExceptionUtils {
  private static final String sInnerCauseKeyName = "grpc-exception-cause-bin";
  private static final Metadata.Key<byte[]> sInnerCauseKey =
      Metadata.Key.of(sInnerCauseKeyName, Metadata.BINARY_BYTE_MARSHALLER);

  private GrpcExceptionUtils() {} // prevent instantiation

  /**
   * Convert from grpc exception to alluxio status exception.
   *
   * @param e the grpc exception
   * @return the alluxio status exception
   */
  public static AlluxioStatusException fromGrpcStatusException(StatusRuntimeException e) {
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

    /**
     * Try to find a source cause for this exception for extracting the message.
     * An embedded cause in exception trailers will always take precedence, over the exception itself.
     * gRPC exception will mostly contain the status code only, unless we had embedded the cause in
     * {@code toGrpcStatusException()) helper.
     */
    Throwable cause = (e.getCause() != null) ? e.getCause() : e;
    if (e.getTrailers() != null && e.getTrailers().containsKey(sInnerCauseKey)) {
      cause = (Throwable) SerializationUtils.deserialize(e.getTrailers().get(sInnerCauseKey));
    }
    return AlluxioStatusException.from(alluxioStatus, cause.getMessage());
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
    /**
     * gRPC does not persist root causes that are persisted with Status.withCause().
     * It's out job to transfer inner exception using metadata trailers.
     */
    Metadata trailers = new Metadata();
    // Embed the exception itself if it doesn't have a cause.
    Throwable cause = (e.getCause() != null) ? e.getCause() : e;
    trailers.put(sInnerCauseKey, SerializationUtils.serialize(cause));
    return Status.fromCode(code).asException(trailers);
  }

  /**
   * Converts a throwable to a gRPC exception.
   *
   * @param e throwable
   * @return gRPC exception
   */
  public static StatusException fromThrowable(Throwable e) {
    return toGrpcStatusException(AlluxioStatusException.fromThrowable(e));
  }
}

