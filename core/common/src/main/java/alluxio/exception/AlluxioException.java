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

import alluxio.thrift.AlluxioTException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * General {@link AlluxioException} used throughout the system. It must be able serialize itself to
 * the RPC framework and convert back without losing any necessary information.
 */
@ThreadSafe
public class AlluxioException extends Exception {
  private static final long serialVersionUID = 2243833925609642384L;

  /**
   * Constructs a {@link AlluxioException} with an exception type from a {@link AlluxioTException}.
   *
   * @param te the type of the exception
   */
  protected AlluxioException(AlluxioTException te) {
    super(te.getMessage());
  }

  /**
   * Constructs an {@link AlluxioException} with the given cause.
   *
   * @param cause the cause
   */
  protected AlluxioException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs an {@link AlluxioException} with the given message.
   *
   * @param message the message
   */
  protected AlluxioException(String message) {
    super(message);
  }

  /**
   * Constructs an {@link AlluxioException} with the given message and cause.
   *
   * @param message the message
   * @param cause the cause
   */
  protected AlluxioException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a {@link AlluxioTException} from a {@link AlluxioException}.
   *
   * @return a {@link AlluxioTException} of the type of this exception
   */
  public AlluxioTException toThrift() {
    return new AlluxioTException(AlluxioExceptionType.getAlluxioExceptionType(getClass()),
        getMessage(), getClass().getName());
  }

  /**
   * Constructs a {@link AlluxioException} from a {@link AlluxioTException}.
   *
   * @param e the {@link AlluxioTException} to convert to a {@link AlluxioException}
   * @return a {@link AlluxioException} of the type specified in e, with the message specified in e
   */

  /**
   * Converts an Alluxio exception from Thrift representation to native representation.
   *
   * @param e the Alluxio Thrift exception
   * @return the native Alluxio exception
   */
  public static AlluxioException fromThrift(AlluxioTException e) {
    try {
      Class<? extends AlluxioException> throwClass;
      if (e.isSetClassName()) {
        // server version 1.1.0 or newer
        throwClass = (Class<? extends AlluxioException>) Class.forName(e.getClassName());
      } else {
        // server version 1.0.x
        throwClass = AlluxioExceptionType.getAlluxioExceptionClass(e.getType());
      }
      if (throwClass == null) {
        throwClass = AlluxioException.class;
      }
      return throwClass.getConstructor(String.class).newInstance(e.getMessage());
    } catch (ReflectiveOperationException reflectException) {
      String errorMessage = "Could not instantiate " + e.getType() + " with a String-only "
          + "constructor: " + reflectException.getMessage();
      throw new IllegalStateException(errorMessage, reflectException);
    }
  }

  /**
   * Holds the different types of exceptions thrown by Alluxio.
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @ThreadSafe
  @Deprecated
  private enum AlluxioExceptionType {
    ACCESS_CONTROL(AccessControlException.class),
    BLOCK_ALREADY_EXISTS(BlockAlreadyExistsException.class),
    BLOCK_DOES_NOT_EXIST(BlockDoesNotExistException.class),
    BLOCK_INFO(BlockInfoException.class),
    CONNECTION_FAILED(ConnectionFailedException.class),
    DEPENDENCY_DOES_NOT_EXIST(DependencyDoesNotExistException.class),
    DIRECTORY_NOT_EMPTY_EXCEPTION(DirectoryNotEmptyException.class),
    FAILED_TO_CHECKPOINT(FailedToCheckpointException.class),
    FILE_ALREADY_COMPLETED(FileAlreadyCompletedException.class),
    FILE_ALREADY_EXISTS(FileAlreadyExistsException.class),
    FILE_DOES_NOT_EXIST(FileDoesNotExistException.class),
    INVALID_FILE_SIZE(InvalidFileSizeException.class),
    INVALID_PATH(InvalidPathException.class),
    INVALID_WORKER_STATE(InvalidWorkerStateException.class),
    LINEAGE_DELETION(LineageDeletionException.class),
    LINEAGE_DOES_NOT_EXIST(LineageDoesNotExistException.class),
    NO_WORKER(NoWorkerException.class),
    WORKER_OUT_OF_SPACE(WorkerOutOfSpaceException.class);

    private final Class<? extends AlluxioException> mExceptionClass;

    /**
     * Constructs a {@link AlluxioExceptionType} with its corresponding {@link AlluxioException}.
     */
    AlluxioExceptionType(Class<? extends AlluxioException> exceptionClass) {
      mExceptionClass = exceptionClass;
    }

    /**
     * Produces an Alluxio exception whose type matches the given name.
     *
     * @param text the type name
     * @return the Alluxio exception
     */
    static Class<? extends AlluxioException> getAlluxioExceptionClass(String text) {
      if (text != null) {
        for (AlluxioExceptionType t : AlluxioExceptionType.values()) {
          if (text.equalsIgnoreCase(t.name())) {
            return t.mExceptionClass;
          }
        }
      }
      return null;
    }

    /**
     * Produces the type name for the type that matches the given Alluxio exception.
     *
     * @param e the Alluxio exception
     * @return the type name
     */
    static String getAlluxioExceptionType(Class<? extends AlluxioException> e) {
      if (e != null) {
        for (AlluxioExceptionType t : AlluxioExceptionType.values()) {
          if (t.mExceptionClass.equals(e)) {
            return t.name();
          }
        }
      }
      return null;
    }
  }
}
