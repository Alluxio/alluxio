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
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.exception.NoWorkerException;
import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.thrift.AlluxioTException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.attribute.UserPrincipalNotFoundException;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.concurrent.RejectedExecutionException;

/**
 * An exception thrown by Alluxio. {@link #getStatus()} can be used to determine the represented
 * class of error.
 */
public class AlluxioStatusException extends RuntimeException {
  private static final long serialVersionUID = -7422144873058169662L;

  private final ExceptionStatus mStatus;

  /**
   * @param status the status code for this exception
   * @param message the exception message
   */
  public AlluxioStatusException(ExceptionStatus status, String message) {
    super(message);
    mStatus = status;
  }

  /**
   * @param status the status code for this exception
   * @param cause the cause of the exception
   */
  public AlluxioStatusException(ExceptionStatus status, Throwable cause) {
    this(status, cause.getMessage(), cause);
  }

  /**
   * @param status the status code for this exception
   * @param message the exception message
   * @param cause the cause of the exception
   */
  public AlluxioStatusException(ExceptionStatus status, String message, Throwable cause) {
    super(message, cause);
    mStatus = status;
  }

  /**
   * @return the status code for this exception
   */
  public ExceptionStatus getStatus() {
    return mStatus;
  }

  /**
   * @return the Thrift representation of this exception
   */
  public AlluxioTException toThrift() {
    return new AlluxioTException(getMessage(), ExceptionStatus.toThrift(mStatus));
  }

  /**
   * Converts an Alluxio exception from Thrift representation to native representation.
   *
   * @param e the Alluxio Thrift exception
   * @return the native Alluxio exception
   */
  public static AlluxioStatusException fromThrift(AlluxioTException e) {
    String m = e.getMessage();
    switch (e.getStatus()) {
      case CANCELED:
        return new CanceledException(m);
      case INVALID_ARGUMENT:
        return new InvalidArgumentException(m);
      case DEADLINE_EXCEEDED:
        return new DeadlineExceededException(m);
      case NOT_FOUND:
        return new NotFoundException(m);
      case ALREADY_EXISTS:
        return new AlreadyExistsException(m);
      case PERMISSION_DENIED:
        return new PermissionDeniedException(m);
      case UNAUTHENTICATED:
        return new UnauthenticatedException(m);
      case RESOURCE_EXHAUSTED:
        return new ResourceExhaustedException(m);
      case FAILED_PRECONDITION:
        return new FailedPreconditionException(m);
      case ABORTED:
        return new AbortedException(m);
      case OUT_OF_RANGE:
        return new OutOfRangeException(m);
      case UNIMPLEMENTED:
        return new UnimplementedException(m);
      case INTERNAL:
        return new InternalException(m);
      case UNAVAILABLE:
        return new UnavailableException(m);
      case DATA_LOSS:
        return new DataLossException(m);
      default:
        return new UnknownException(m);
    }
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
    } catch (BlockDoesNotExistException | FileDoesNotExistException
        | LineageDoesNotExistException e) {
      return new NotFoundException(e);
    } catch (BlockInfoException | InvalidFileSizeException | InvalidPathException e) {
      return new InvalidArgumentException(e);
    } catch (ConnectionFailedException | FailedToCheckpointException | NoWorkerException
        | UfsBlockAccessTokenUnavailableException e) {
      return new UnavailableException(e);
    } catch (DependencyDoesNotExistException | DirectoryNotEmptyException
        | InvalidWorkerStateException | LineageDeletionException e) {
      return new FailedPreconditionException(e);
    } catch (WorkerOutOfSpaceException e) {
      return new ResourceExhaustedException(e);
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
      throw new NotFoundException(e);
    } catch (MalformedURLException e) {
      throw new InvalidArgumentException(e);
    } catch (UserPrincipalNotFoundException e) {
      throw new UnauthenticatedException(e);
    } catch (IOException e) {
      throw new UnavailableException(e);
    }
  }

  /**
   * Converts runtime exceptions to Alluxio status exceptions. Well-known runtime exceptions are
   * converted intelligently. Unrecognized runtime exceptions are converted to
   * {@link UnknownException}.
   *
   * @param re the runtime exception to convert
   * @return the converted {@link AlluxioStatusException}
   */
  public static AlluxioStatusException fromRuntimeException(RuntimeException re) {
    try {
      throw re;
    } catch (ArithmeticException | ClassCastException | ConcurrentModificationException
        | IllegalStateException | NoSuchElementException | NullPointerException
        | UnsupportedOperationException e) {
      return new InternalException(e);
    } catch (IllegalArgumentException e) {
      return new InvalidArgumentException(e);
    } catch (IndexOutOfBoundsException e) {
      return new OutOfRangeException(e);
    } catch (RejectedExecutionException e) {
      return new ResourceExhaustedException(e);
    } catch (RuntimeException e) {
      return new UnknownException(e);
    }
  }
}
