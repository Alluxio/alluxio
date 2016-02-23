/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.exception;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Holds the different types of exceptions thrown by Alluxio.
 */
@ThreadSafe
public enum AlluxioExceptionType {
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
  WORKER_OUT_OF_SPACE(WorkerOutOfSpaceException.class)
  ;

  private final Class<? extends AlluxioException> mExceptionClass;

  /**
   * Constructs a {@link AlluxioExceptionType} with its corresponding {@link AlluxioException}.
   */
  AlluxioExceptionType(Class<? extends AlluxioException> exceptionClass) {
    mExceptionClass = exceptionClass;
  }

  /**
   * Gets the class of the exception.
   *
   * @return the class of the exception
   */
  public Class<? extends AlluxioException> getExceptionClass() {
    return mExceptionClass;
  }
}
