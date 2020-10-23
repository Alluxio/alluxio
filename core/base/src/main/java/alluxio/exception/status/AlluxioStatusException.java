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

package alluxio.exception.status;

import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BackupAbortedException;
import alluxio.exception.BackupDelegationException;
import alluxio.exception.BackupException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.DependencyDoesNotExistException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FailedToCheckpointException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.exception.WorkerOutOfSpaceException;

import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import javax.security.sasl.SaslException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.attribute.UserPrincipalNotFoundException;

/**
 * An exception thrown by Alluxio. {@link #getStatusCode()} can be used to determine the represented
 * class of error.
 */
public class AlluxioStatusException extends IOException {
  private static final long serialVersionUID = -7422144873058169662L;

  private final Status mStatus;

  /**
   * @param status the status code for this exception
   */
  public AlluxioStatusException(Status status) {
    super(status.getDescription(), status.getCause());
    mStatus = status;
  }

  /**
   * @return the internal status for this exception
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @return the status code for this exception
   */
  public Status.Code getStatusCode() {
    return mStatus.getCode();
  }

  /**
   * @return a specific {@link AlluxioException} corresponding to this exception if there is one;
   *         otherwise return a generic {@link AlluxioException}
   */
  public AlluxioException toAlluxioException() {
    switch (mStatus.getCode()) {
      // Fall throughs are intentional.
      case PERMISSION_DENIED:
      case UNAUTHENTICATED:
        return new AccessControlException(getMessage(), this);
      case ABORTED:
      case ALREADY_EXISTS:
      case CANCELLED:
      case DATA_LOSS:
      case DEADLINE_EXCEEDED:
      case FAILED_PRECONDITION:
      case INTERNAL:
      case INVALID_ARGUMENT:
      case NOT_FOUND:
      case OUT_OF_RANGE:
      case RESOURCE_EXHAUSTED:
      case UNAVAILABLE:
      case UNIMPLEMENTED:
      case UNKNOWN:
      default:
        return new AlluxioException(getMessage(), this);
    }
  }

  /**
   * @return a gRPC status exception representation of this exception
   */
  public StatusException toGrpcStatusException() {
    return mStatus.asException();
  }

  /**
   * Converts an Alluxio exception from status and message representation to native representation.
   * The status must not be null or {@link Status#OK}.
   *
   * @param status the status
   * @return an {@link AlluxioStatusException} for the given status and message
   */
  public static AlluxioStatusException from(Status status) {
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(status != Status.OK, "OK is not an error status");
    final String message = status.getDescription();
    final Throwable cause = status.getCause();
    switch (status.getCode()) {
      case CANCELLED:
        return new CancelledException(message, cause);
      case INVALID_ARGUMENT:
        return new InvalidArgumentException(message, cause);
      case DEADLINE_EXCEEDED:
        return new DeadlineExceededException(message, cause);
      case NOT_FOUND:
        return new NotFoundException(message, cause);
      case ALREADY_EXISTS:
        return new AlreadyExistsException(message, cause);
      case PERMISSION_DENIED:
        return new PermissionDeniedException(message, cause);
      case UNAUTHENTICATED:
        return new UnauthenticatedException(message, cause);
      case RESOURCE_EXHAUSTED:
        return new ResourceExhaustedException(message, cause);
      case FAILED_PRECONDITION:
        return new FailedPreconditionException(message, cause);
      case ABORTED:
        return new AbortedException(message, cause);
      case OUT_OF_RANGE:
        return new OutOfRangeException(message, cause);
      case UNIMPLEMENTED:
        return new UnimplementedException(message, cause);
      case INTERNAL:
        return new InternalException(message, cause);
      case UNAVAILABLE:
        return new UnavailableException(message, cause);
      case DATA_LOSS:
        return new DataLossException(message, cause);
      default:
        Throwable nestedCause = cause;
        while (nestedCause != null) {
          if (nestedCause instanceof ClosedChannelException) {
            // GRPC can mask closed channels as unknown exceptions, but unavailable is more
            // appropriate
            return new UnavailableException(message, cause);
          }
          nestedCause = nestedCause.getCause();
        }
        return new UnknownException(message, cause);
    }
  }

  /**
   * Converts checked throwables to Alluxio status exceptions. Unchecked throwables should not be
   * passed to this method. Use Throwables.propagateIfPossible before passing a Throwable to this
   * method.
   *
   * @param throwable a throwable
   * @return the converted {@link AlluxioStatusException}
   */
  public static AlluxioStatusException fromCheckedException(Throwable throwable) {
    try {
      throw throwable;
    } catch (IOException e) {
      return fromIOException(e);
    } catch (AlluxioException e) {
      return fromAlluxioException(e);
    } catch (InterruptedException e) {
      return new CancelledException(e);
    } catch (RuntimeException e) {
      throw new IllegalStateException("Expected a checked exception but got " + e);
    } catch (Exception e) {
      return new UnknownException(e);
    } catch (Throwable t) {
      throw new IllegalStateException("Expected a checked exception but got " + t);
    }
  }

  /**
   * Converts an arbitrary throwable to an Alluxio status exception. This method should be used with
   * caution because it could potentially convert an unchecked exception (indicating a bug) to a
   * checked Alluxio status exception.
   *
   * @param t a throwable
   * @return the converted {@link AlluxioStatusException}
   */
  public static AlluxioStatusException fromThrowable(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      return fromStatusRuntimeException((StatusRuntimeException) t);
    }
    if (t instanceof Error || t instanceof RuntimeException) {
      return new InternalException(t);
    }
    return fromCheckedException(t);
  }

  /**
   * Converts a gRPC StatusRuntimeException to an Alluxio status exception.
   *
   * @param e a gRPC StatusRuntimeException
   * @return the converted {@link AlluxioStatusException}
   */
  public static AlluxioStatusException fromStatusRuntimeException(StatusRuntimeException e) {
    return AlluxioStatusException.from(e.getStatus().withCause(e));
  }

  /**
   * Converts checked Alluxio exceptions to Alluxio status exceptions.
   *
   * @param ae the Alluxio exception to convert
   * @return the converted {@link AlluxioStatusException}
   */
  public static AlluxioStatusException fromAlluxioException(AlluxioException ae) {
    try {
      throw ae;
    } catch (AccessControlException e) {
      return new PermissionDeniedException(e);
    } catch (BlockAlreadyExistsException | FileAlreadyCompletedException
        | FileAlreadyExistsException e) {
      return new AlreadyExistsException(e);
    } catch (BlockDoesNotExistException | FileDoesNotExistException | JobDoesNotExistException e) {
      return new NotFoundException(e);
    } catch (BlockInfoException | InvalidFileSizeException | InvalidPathException e) {
      return new InvalidArgumentException(e);
    } catch (ConnectionFailedException | FailedToCheckpointException
        | UfsBlockAccessTokenUnavailableException e) {
      return new UnavailableException(e);
    } catch (DependencyDoesNotExistException | DirectoryNotEmptyException
        | InvalidWorkerStateException e) {
      return new FailedPreconditionException(e);
    } catch (WorkerOutOfSpaceException e) {
      return new ResourceExhaustedException(e);
    } catch (BackupDelegationException e) {
      return new FailedPreconditionException(e);
    } catch (BackupAbortedException e) {
      return new AbortedException(e);
    } catch (BackupException e) {
      return new InternalException(e);
    } catch (AlluxioException e) {
      return new UnknownException(e);
    }
  }

  /**
   * Converts an IOException to a corresponding status exception. Unless special cased, IOExceptions
   * are converted to {@link UnavailableException}.
   *
   * @param ioe the IO exception to convert
   * @return the corresponding status exception
   */
  public static AlluxioStatusException fromIOException(IOException ioe) {
    try {
      throw ioe;
    } catch (FileNotFoundException e) {
      return new NotFoundException(e);
    } catch (MalformedURLException e) {
      return new InvalidArgumentException(e);
    } catch (UserPrincipalNotFoundException e) {
      return new UnauthenticatedException(e);
    } catch (ClosedChannelException e) {
      return new FailedPreconditionException(e);
    } catch (AlluxioStatusException e) {
      return e;
    } catch (SaslException e) {
      return new UnauthenticatedException(e);
    } catch (IOException e) {
      return new UnknownException(e);
    }
  }
}
