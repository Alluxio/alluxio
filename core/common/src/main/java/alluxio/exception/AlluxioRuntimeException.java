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

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.FailedPreconditionRuntimeException;
import alluxio.exception.status.InvalidArgumentRuntimeException;
import alluxio.exception.status.NotFoundRuntimeException;
import alluxio.exception.status.UnauthenticatedRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.exception.status.UnknownRuntimeException;
import alluxio.grpc.RetryInfo;

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.ProtoUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.attribute.UserPrincipalNotFoundException;
import javax.security.sasl.SaslException;

/**
 * Alluxio RuntimeException. Every developer should throw this exception when need to surface
 * exception to client.
 */
public class AlluxioRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 7801880681732804395L;
  private final Status mStatus;
  private final Any[] mDetails;
  private final boolean mRetryable;

  /**
   * @param status  the grpc status code for this exception
   * @param message the error message
   * @param details the additional information needed
   * @param retryable client can retry or not
   */
  public AlluxioRuntimeException(Status status, String message, boolean retryable,
      Any... details) {
    this(status, message, null, retryable, details);
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
   * @param cause   the exception
   * @param details the additional information needed
   */
  public AlluxioRuntimeException(Status status, Throwable cause, Any... details) {
    this(status, null, cause, false, details);
  }

  /**
   * @param status  the grpc status code for this exception
   * @param message the error message
   * @param cause   the exception
   * @param retryable client can retry or not
   * @param details the additional information needed
   */
  public AlluxioRuntimeException(Status status, String message, Throwable cause,
      boolean retryable, Any... details) {
    super(message, cause);
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(status != Status.OK, "OK is not an error status");
    mStatus = status.withCause(cause).withDescription(message);
    mDetails = details;
    mRetryable = retryable;
  }

  /**
   * @return grpc status
   */
  public Status getStatus() {
    return mStatus;
  }

    /**
   * @return can be retried or not
   */
  public boolean isRetryable() {
    return mRetryable;
  }

  /**
   * @return a gRPC status exception representation of this exception
   */
  public StatusException toGrpcStatusException() {
    Metadata trailers = new Metadata();
    trailers.put(ProtoUtils.keyForProto(RetryInfo.getDefaultInstance()),
        RetryInfo.newBuilder().setIsRetryable(mRetryable).build());
    if (mDetails != null) {
      trailers = new Metadata();
      for (Any details : mDetails) {
        trailers.put(ProtoUtils.keyForProto(Any.getDefaultInstance()), details);
      }
    }
    return mStatus.withCause(getCause()).withDescription(getMessage()).asException(trailers);
  }

  /**
   * Converts an arbitrary throwable to an Alluxio runtime exception.
   * @param t exception
   * @return alluxio runtime exception
   */
  public static AlluxioRuntimeException from(Throwable t) {
    if (t instanceof AlluxioRuntimeException) {
      return (AlluxioRuntimeException) t;
    }
    if (t instanceof AlluxioStatusException) {
      return new AlluxioRuntimeException(((AlluxioStatusException) t).getStatus(), t.getMessage(),
          t.getCause(), false);
    }
    if (t instanceof IOException) {
      return fromIOException((IOException) t);
    }
    return new UnknownRuntimeException(t);
  }

    /**
   * Converts an IOException to a corresponding status exception. Unless special cased, IOExceptions
   * are converted to {@link UnavailableException}.
   *
   * @param ioe the IO exception to convert
   * @return the corresponding status exception
   */
  public static AlluxioRuntimeException fromIOException(IOException ioe) {
    if (ioe instanceof FileNotFoundException) {
      return new NotFoundRuntimeException(ioe);
    }
    if (ioe instanceof MalformedURLException) {
      return new InvalidArgumentRuntimeException(ioe);
    }
    if (ioe instanceof UserPrincipalNotFoundException || ioe instanceof SaslException) {
      return new UnauthenticatedRuntimeException(ioe);
    }
    if (ioe instanceof ClosedChannelException) {
      return new FailedPreconditionRuntimeException(ioe);
    }
    return new UnknownRuntimeException(ioe);
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
