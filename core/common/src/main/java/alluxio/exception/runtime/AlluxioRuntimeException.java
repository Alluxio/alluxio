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

package alluxio.exception.runtime;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.ErrorInfo;
import alluxio.grpc.ErrorType;
import alluxio.grpc.RetryInfo;

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.attribute.UserPrincipalNotFoundException;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import javax.security.sasl.SaslException;

/**
 * Alluxio RuntimeException. This Every developer should throw this kind of exception
 * when you really need to throw exception and surface this exception to user or when you want to
 * catch low level exception from external dependency and transform it into AlluxioRuntimeException.
 *
 * There are 3 ways of using this class here:
 * 1. Carefully pick a convenient class like {@link InternalRuntimeException}.
 * 2. Throw new AlluxioRuntimeException directly.
 * 3. Try not to use AlluxioRuntimeException.from(cause). This method used when you catch a very
 *    general cause(ex.{@link IOException}) and is mainly for compatibility
 */
public class AlluxioRuntimeException extends RuntimeException {
  private final Status mStatus;
  private final Any[] mDetails;
  private final boolean mRetryable;
  private final ErrorType mErrorType;

  /**
   * @param status  the grpc status code for this exception
   * @param message the error message
   * @param cause   the exception
   * @param errorType error type
   * @param retryable client can retry or not
   * @param details the additional information needed
   */
  public AlluxioRuntimeException(Status status, String message, @Nullable Throwable cause,
      ErrorType errorType, boolean retryable, @Nullable Any... details) {
    super(message, cause);
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(status != Status.OK, "OK is not an error status");
    mStatus = status.withCause(cause).withDescription(message);
    mDetails = details;
    mRetryable = retryable;
    mErrorType =  errorType;
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
    return mStatus.withCause(getCause()).withDescription(getMessage()).asException(getMetadata());
  }

  private Metadata getMetadata() {
    Metadata trailers = new Metadata();
    trailers.put(ProtoUtils.keyForProto(RetryInfo.getDefaultInstance()),
        RetryInfo.newBuilder().setIsRetryable(mRetryable).build());
    trailers.put(ProtoUtils.keyForProto(ErrorInfo.getDefaultInstance()),
        ErrorInfo.newBuilder().setErrorType(mErrorType).build());
    if (mDetails != null) {
      for (Any detail : mDetails) {
        trailers.put(ProtoUtils.keyForProto(Any.getDefaultInstance()), detail);
      }
    }
    return trailers;
  }

  /**
   * @return a gRPC status runtime exception representation of this exception
   */
  public StatusRuntimeException toGrpcStatusRuntimeException() {
    return mStatus.withCause(getCause()).withDescription(getMessage())
        .asRuntimeException(getMetadata());
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
      return from((AlluxioStatusException) t);
    }
    if (t instanceof IOException) {
      return from((IOException) t);
    }
    if (t instanceof RuntimeException) {
      return from((RuntimeException) t);
    }
    return new UnknownRuntimeException(t);
  }

  /**
   * Converts an arbitrary RuntimeException to an Alluxio runtime exception.
   * @param t exception
   * @return alluxio runtime exception
   */
  public static AlluxioRuntimeException from(RuntimeException t) {
    if (t instanceof IllegalArgumentException) {
      return new InvalidArgumentRuntimeException(t);
    }
    if (t instanceof IllegalStateException) {
      return new InternalRuntimeException(t);
    }
    if (t instanceof CompletionException) {
      return from(t.getCause());
    }
    if (t instanceof UnsupportedOperationException) {
      return new UnimplementedRuntimeException(t, ErrorType.External);
    }
    if (t instanceof SecurityException) {
      return new PermissionDeniedRuntimeException(t);
    }
    return new UnknownRuntimeException(t);
  }

  /**
   * Converts an arbitrary AlluxioStatusException to an Alluxio runtime exception.
   * @param t exception
   * @return alluxio runtime exception
   */
  public static AlluxioRuntimeException from(AlluxioStatusException t) {
    return new AlluxioRuntimeException(t.getStatus(), t.getMessage(), t.getCause(), ErrorType.User,
        false);
  }

  /**
   * Converts an IOException to a corresponding runtime exception.
   *
   * @param ioe the IO exception to convert
   * @return the corresponding status exception
   */
  public static AlluxioRuntimeException from(IOException ioe) {
    if (ioe instanceof AlluxioStatusException) {
      return from((AlluxioStatusException) ioe);
    }
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
