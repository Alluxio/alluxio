/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.exception;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Holds the different types of exceptions thrown by Tachyon.
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
