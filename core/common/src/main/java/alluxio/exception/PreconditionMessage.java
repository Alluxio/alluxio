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
 * Precondition messages used across Alluxio.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in this section.
 */
@ThreadSafe
public final class PreconditionMessage {

  public static final String CANNOT_READ_FOLDER = "Cannot read from a folder";
  public static final String CLIENT_CONTEXT_NOT_INITIALIZED = "Client Context not initialized";
  public static final String COMMAND_LINE_LINEAGE_ONLY =
      "Only command line jobs are supported by createLineage";
  public static final String EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK =
      "The passed-in file info list can not be empty when checking permission";
  public static final String ERR_BLOCK_INDEX = "Current block index exceeds max index";
  public static final String ERR_BLOCK_REMAINING =
      "The current block still has space left, no need to get new block";
  public static final String ERR_BUFFER_STATE = "Buffer length: %s, offset: %s, len: %s";
  public static final String ERR_CLOSED_BLOCK_IN_STREAM =
      "Cannot do operations on a closed BlockInStream";
  public static final String ERR_CLOSED_BLOCK_OUT_STREAM =
      "Cannot do operations on a closed BlockOutStream";
  public static final String ERR_END_OF_BLOCK = "Cannot write past end of block";
  public static final String ERR_READ_BUFFER_NULL = "Read buffer cannot be null";
  public static final String ERR_SEEK_NEGATIVE = "Seek position is negative: %s";
  public static final String ERR_SEEK_PAST_END_OF_BLOCK = "Seek position past end of block: %s";
  public static final String ERR_SEEK_PAST_END_OF_FILE = "Seek position past end of file: %s";
  public static final String ERR_WRITE_BUFFER_NULL = "Cannot write a null input buffer";
  public static final String FILE_WRITE_LOCATION_POLICY_UNSPECIFIED =
      "The location policy is not specified";
  public static final String INODE_TREE_UNINITIALIZED_IS_ROOT_ID =
      "Cannot call isRootId() before initializeRoot()";
  public static final String INVALID_SET_ACL_OPTIONS =
      "Invalid set acl options: %s, %s, %s";
  public static final String LINEAGE_DOES_NOT_EXIST = "Lineage id %s does not exist";
  public static final String LINEAGE_NO_OUTPUT_FILE =
      "The output file %s is not associated with any lineage";
  public static final String MUST_SET_PINNED = "The pinned flag must be set";
  public static final String MUST_SET_TTL = "The TTL value must be set";
  public static final String MUST_SET_PERSISTED = "The persisted value must be set";
  public static final String MUST_SET_OWNER = "The owner must be set";
  public static final String MUST_SET_GROUP = "The group must be set";
  public static final String MUST_SET_PERMISSION = "The permission must be set";
  public static final String MUST_SET_RECURSIVE = "The recursive must be set";
  public static final String REMOTE_CLIENT_BUT_LOCAL_HOSTNAME =
      "Acquire Remote Worker Client cannot not be called with local hostname";
  public static final String S3_BUCKET_MUST_BE_SET =
      "The %s system property must be set to use the S3UnderStorageCluster";
  public static final String GCS_BUCKET_MUST_BE_SET =
      "The %s system property must be set to use the GCSUnderStorageCluster";
  public static final String TTL_ONLY_FOR_FILE = "TTL can only be set for files";
  public static final String PERSIST_ONLY_FOR_FILE = "Only files can be persisted";
  public static final String FILE_TO_PERSIST_MUST_BE_COMPLETE =
      "File being persisted must be complete";
  public static final String ERR_SET_STATE_UNPERSIST =
      "Cannot set the state of a file to not-persisted";
  public static final String URI_HOST_NULL = "URI hostname must not be null";
  public static final String URI_PORT_NULL = "URI port must not be null";
  public static final String URI_KEY_VALUE_STORE_NULL = "URI of key-value store must not be null";
  public static final String ERR_PUT_EMPTY_KEY = "Cannot put an empty buffer as a key";
  public static final String ERR_PUT_EMPTY_VALUE = "Cannot put an empty buffer as a value";
  public static final String ERR_PUT_NULL_KEY = "Cannot put a null key";
  public static final String ERR_PUT_NULL_VALUE = "Cannot put a null value";

  private PreconditionMessage() {} // to prevent initialization
}
