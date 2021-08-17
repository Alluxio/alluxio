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

import javax.annotation.concurrent.ThreadSafe;

/**
 * Precondition messages used across Alluxio.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in this section.
 */
@ThreadSafe
public enum PreconditionMessage {
  BLOCK_LENGTH_INCONSISTENT("Block %s is expected to be %s bytes, but only %s bytes are available. "
      + "Please ensure its metadata is consistent between Alluxio and UFS."),
  EMPTY_FILE_INFO_LIST_FOR_PERMISSION_CHECK(
      "The passed-in file info list can not be empty when checking permission"),
  ERR_BLOCK_REMAINING("The current block still has space left, no need to get new block"),
  ERR_BUFFER_STATE("Buffer length: %s, offset: %s, len: %s"),
  ERR_CLOSED_BLOCK_IN_STREAM("Cannot do operations on a closed BlockInStream"),
  ERR_CLOSED_BLOCK_OUT_STREAM("Cannot do operations on a closed BlockOutStream"),
  ERR_CLOSED_UNDER_FILE_SYSTEM_FILE_OUT_STREAM(
      "Cannot do operations on a closed UnderFileSystemFileOutStream"),
  ERR_END_OF_BLOCK("Cannot write past end of block"),
  ERR_READ_BUFFER_NULL("Read buffer cannot be null"),
  ERR_PUT_EMPTY_KEY("Cannot put an empty buffer as a key"),
  ERR_PUT_EMPTY_VALUE("Cannot put an empty buffer as a value"),
  ERR_PUT_NULL_KEY("Cannot put a null key"),
  ERR_PUT_NULL_VALUE("Cannot put a null value"),
  ERR_SEEK_NEGATIVE("Seek position is negative: %s"),
  ERR_SEEK_PAST_END_OF_BLOCK("Seek position past end of block: %s"),
  ERR_SEEK_PAST_END_OF_FILE("Seek position past end of file: %s"),
  ERR_SEEK_PAST_END_OF_REGION("Seek position past the end of the read region (block or file)."),
  ERR_SET_STATE_UNPERSIST("Cannot set the state of a file to not-persisted"),
  ERR_TIER_QUOTA_BLANK("Tier capacity configuration should not be blank"),
  ERR_UFS_MANAGER_OPERATION_INVALID_SESSION("Attempted to %s ufs file with invalid session id."),
  ERR_UFS_MANAGER_FAILED_TO_REMOVE_AGENT(
      "Failed to remove agent %s from ufs manager's internal state."),
  ERR_UNEXPECTED_EOF("Reached EOF unexpectedly."),
  ERR_WRITE_BUFFER_NULL("Cannot write a null input buffer"),
  ERR_ZK_ADDRESS_NOT_SET("Cannot get leader address from zookeeper; %s is not set"),
  FILE_TO_PERSIST_MUST_BE_COMPLETE("File being persisted must be complete"),
  BLOCK_WRITE_LOCATION_POLICY_UNSPECIFIED("The location policy is not specified"),
  UFS_READ_LOCATION_POLICY_UNSPECIFIED("The UFS read location policy is not specified"),
  GCS_BUCKET_MUST_BE_SET("The %s system property must be set to use the GCSUnderStorageCluster"),
  INCONSISTENT_ZK_CONFIGURATION(
      "Inconsistent Zookeeper configuration; %s should be set if and only if %s is true"),
  INODE_TREE_UNINITIALIZED_IS_ROOT_ID("Cannot call isRootId() before initializeRoot()"),
  INVALID_REPLICATION_MAX_VALUE("Max replication must be -1 (infinity) or a non-negative integer"),
  INVALID_REPLICATION_MIN_VALUE("Min replication must be a non-negative integer"),
  INVALID_REPLICATION_MAX_MIN_VALUE_NULL("Both min and max replication are null"),
  INVALID_REPLICATION_MAX_SMALLER_THAN_MIN("Cannot set min and max replication to be %s and %s: "
      + "min replication must be smaller or equal than max replication"),
  INVALID_PROPERTY("Invalid property: %s"),
  INVALID_USER_FILE_BUFFER_BYTES("Invalid value of %s: %s"),
  NOT_ENOUGH_BYTES_READ(
      "Not enough bytes have been read [bytesRead: %s, bytesToRead: %s] from the UFS file: %s."),
  PERMISSION_GROUP_IS_NULL("Group cannot be null when constructing Permission"),
  PERMISSION_MODE_IS_NULL("Mode cannot be null when constructing Permission"),
  PERMISSION_OWNER_IS_NULL("Owner cannot be null when constructing Permission"),
  PERSIST_ONLY_FOR_FILE("Only files can be persisted"),
  CHANNEL_NULL_WHEN_CONNECTED(
      "The client channel should never be null when the client is connected"),
  REMOTE_CLIENT_BUT_LOCAL_HOSTNAME(
      "Acquire Remote Worker Client cannot not be called with local hostname"),
  S3_BUCKET_MUST_BE_SET("The %s system property must be set to use the S3UnderStorageCluster"),
  URI_HOST_NULL("URI hostname must not be null"),
  URI_PORT_NULL("URI port must not be null"),
  URI_KEY_VALUE_STORE_NULL("URI of key-value store must not be null"),
  URI_SCHEME_MISMATCH("URI scheme %s does not match the expected scheme %s"),
  // SEMICOLON! minimize merge conflicts by putting it on its own line
  ;

  private final String mMessage;

  PreconditionMessage(String message) {
    mMessage = message;
  }

  @Override
  public String toString() {
    return mMessage;
  }
}
