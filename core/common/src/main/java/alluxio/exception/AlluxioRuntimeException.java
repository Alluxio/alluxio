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

import alluxio.grpc.RetryInfo;

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.ProtoUtils;

/**
 * Alluxio RuntimeException. Every developer should throw this exception when need to surface
 * exception to client.
 */
public class AlluxioRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 7801880681732804395L;
  private final Status mStatus;
  private final Any[] mDetails;
  private final boolean mIsRetryable;

  /**
   * @param status  the grpc status code for this exception
   * @param message the error message
   * @param details the additional information needed
   * @param isRetryable client can retry or not
   */
  public AlluxioRuntimeException(Status status, String message, boolean isRetryable,
      Any... details) {
    this(status, message, null, isRetryable, details);
  }

  /**
   * @param message the error message
   * @param details the additional information needed
   */
  public AlluxioRuntimeException(String message, Any... details) {
    this(Status.UNKNOWN, message, null, false, details);
  }

  /**
   * @param status  the grpc status code for this exception
   * @param message the error message
   * @param cause   the exception
   * @param isRetryable client can retry or not
   * @param details the additional information needed
   */
  public AlluxioRuntimeException(Status status, String message, Throwable cause,
      boolean isRetryable, Any... details) {
    super(message, cause);
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(status != Status.OK, "OK is not an error status");
    mStatus = status.withCause(cause).withDescription(message);
    mDetails = details;
    mIsRetryable = isRetryable;
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
    Metadata trailers = new Metadata();
    trailers.put(ProtoUtils.keyForProto(RetryInfo.getDefaultInstance()),
        RetryInfo.newBuilder().setIsRetryable(mIsRetryable).build());
    if (mDetails != null) {
      trailers = new Metadata();
      for (Any details : mDetails) {
        trailers.put(ProtoUtils.keyForProto(Any.getDefaultInstance()), details);
      }
    }
    return mStatus.withCause(getCause()).withDescription(getMessage()).asException(trailers);
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
