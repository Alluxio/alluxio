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

/**
 * Precondition messages used across Tachyon.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in this section.
 */
public class PreconditionMessage {

  public static final String CANNOT_READ_FOLDER = "Cannot read froma  folder";
  public static final String CLIENT_CONTEXT_NOT_INITIALIZED = "Client Context not initialized";
  public static final String COMMAND_LINE_LINEAGE_ONLY =
      "createLineage only supports command line jobs";
  public static final String ERR_BLOCK_INDEX = "Current block index exceeds max index.";
  public static final String ERR_BLOCK_REMAINING =
      "The current block still has space left, no need to get new block.";
  public static final String ERR_BUFFER_STATE = "Buffer length: %s, offset: %s, len: %s";
  public static final String ERR_CLOSED_BLOCK_IN_STREAM =
      "Cannot do operations on a closed BlockInStream";
  public static final String ERR_CLOSED_BLOCK_OUT_STREAM =
      "Cannot do operations on a closed BlockOutStream.";
  public static final String ERR_END_OF_BLOCK = "Cannot write past end of block.";
  public static final String ERR_READ_BUFFER_NULL = "Read buffer cannot be null";
  public static final String ERR_SEEK_NEGATIVE = "Seek position is negative: %s";
  public static final String ERR_SEEK_PAST_END_OF_BLOCK = "Seek position past end of block: %s";
  public static final String ERR_SEEK_PAST_END_OF_FILE = "Seek position past end of file: %s";
  public static final String ERR_WRITE_BUFFER_NULL = "Cannot write a null input buffer.";
  public static final String REMOTE_CLIENT_BUT_LOCAL_HOSTNAME =
      "Acquire Remote Worker Client cannot not be called with local hostname";
}
