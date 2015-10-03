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
  BLOCK_ALREADY_EXISTS,
  BLOCK_DOES_NOT_EXIST,
  BLOCK_INFO,
  DEPENDENCY_DOES_NOT_EXIST,
  FAILED_TO_CHECKPOINT,
  FILE_ALREADY_EXISTS,
  FILE_DOES_NOT_EXIST,
  INVALID_PATH,
  INVALID_WORKER_STATE,
  LINEAGE_DELETION,
  LINEAGE_DOES_NOT_EXIST,
  NO_WORKER,
  SUSPECTED_FILE_SIZE,
  TABLE_COLUMN,
  TABLE_DOES_NOT_EXIST,
  TABLE_METADATA,
  WORKER_OUT_OF_SPACE,
}
