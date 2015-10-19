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

package tachyon.exception;

public enum TachyonExceptionType {
  BLOCK_ALREADY_EXISTS(BlockAlreadyExistsException.class),
  BLOCK_DOES_NOT_EXIST(BlockDoesNotExistException.class),
  BLOCK_INFO(BlockInfoException.class),
  DEPENDENCY_DOES_NOT_EXIST(DependencyDoesNotExistException.class),
  FAILED_TO_CHECKPOINT(FailedToCheckpointException.class),
  FILE_ALREADY_EXISTS(FileAlreadyExistsException.class),
  FILE_DOES_NOT_EXIST(FileDoesNotExistException.class),
  INVALID_PATH(InvalidPathException.class),
  INVALID_WORKER_STATE(InvalidWorkerStateException.class),
  LINEAGE_DELETION(LineageDeletionException.class),
  LINEAGE_DOES_NOT_EXIST(LineageDoesNotExistException.class),
  NO_WORKER(NoWorkerException.class),
  SUSPECTED_FILE_SIZE(SuspectedFileSizeException.class),
  TABLE_COLUMN(TableColumnException.class),
  TABLE_DOES_NOT_EXIST(TableDoesNotExistException.class),
  TABLE_METADATA(TableMetadataException.class),
  WORKER_OUT_OF_SPACE(WorkerOutOfSpaceException.class);

  private final Class<? extends TachyonException> mExceptionClass;

  /**
   * Construct a TachyonExceptionType along with its corresponding TachyonException
   */
  TachyonExceptionType(Class<? extends TachyonException> exceptionClass) {
    this.mExceptionClass = exceptionClass;
  }

  public Class<? extends TachyonException> getExceptionClass() {
    return mExceptionClass;
  }
}
