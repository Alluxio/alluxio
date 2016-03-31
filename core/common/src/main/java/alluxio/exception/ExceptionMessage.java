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

import com.google.common.base.Preconditions;

import java.text.MessageFormat;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Exception messages used across Alluxio.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in this section.
 */
@ThreadSafe
public enum ExceptionMessage {
  // general
  NOT_SUPPORTED("This method is not supported"),
  PATH_DOES_NOT_EXIST("Path {0} does not exist"),
  PATH_MUST_BE_FILE("Path {0} must be a file"),
  PATH_MUST_BE_DIRECTORY("Path {0} must be a directory"),
  PATH_INVALID("Path {0} is invalid"),

  // general block
  BLOCK_NOT_LOCALLY_AVAILABLE("Block {0} is not available on local machine"),
  BLOCK_UNAVAILABLE("Block {0} is not available in Alluxio"),
  CANNOT_REQUEST_SPACE("Unable to request space from worker"),
  NO_LOCAL_WORKER("Local {0} requested but there is no local worker"),
  NO_WORKER_AVAILABLE_ON_HOST("No Alluxio worker available for host {0}"),
  NO_WORKER_AVAILABLE("No available Alluxio worker found"),

  // block lock manager
  LOCK_ID_FOR_DIFFERENT_BLOCK("lockId {0} is for block {1}, not {2}"),
  LOCK_ID_FOR_DIFFERENT_SESSION("lockId {0} is owned by sessionId {1} not {2}"),
  LOCK_RECORD_NOT_FOUND_FOR_BLOCK_AND_SESSION("no lock is found for blockId {0} for sessionId {1}"),
  LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID("lockId {0} has no lock record"),

  // block metadata manager and view
  BLOCK_META_NOT_FOUND("BlockMeta not found for blockId {0}"),
  GET_DIR_FROM_NON_SPECIFIC_LOCATION("Cannot get path from non-specific dir {0}"),
  TEMP_BLOCK_META_NOT_FOUND("TempBlockMeta not found for blockId {0}"),
  TIER_ALIAS_NOT_FOUND("Tier with alias {0} not found"),
  TIER_VIEW_ALIAS_NOT_FOUND("Tier view with alias {0} not found"),

  // instream/outstream
  FAILED_CACHE("Failed to cache: {0}"),
  FAILED_CREATE("Failed to create {0}"),
  FAILED_SEEK_FORWARD("Failed to seek forward to {0}"),
  FAILED_SKIP("Failed to skip {0}"),
  INSTREAM_CANNOT_SKIP("The underlying BlockInStream could not skip {0}"),
  READ_CLOSED_STREAM("Cannot read from a closed stream"),
  SEEK_NEGATIVE("Seek position is negative: {0}"),
  SEEK_PAST_EOF("Seek position is past EOF: {0}, fileSize: {1}"),

  // netty
  BLOCK_WRITE_ERROR("Error writing blockId: {0}, sessionId: {1}, address: {2}, message: {3}"),
  NO_RPC_HANDLER("No handler implementation for rpc message type: {0}"),
  UNEXPECTED_RPC_RESPONSE("Unexpected response message type: {0} (expected: {1})"),
  WRITER_ALREADY_OPEN("This writer is already open for address: {0}, blockId: {1}, sessionId: {2}"),

  // storageDir
  ADD_EXISTING_BLOCK("blockId {0} exists in {1}"),
  BLOCK_NOT_FOUND_FOR_SESSION("blockId {0} in {1} not found for session {2}"),
  NO_SPACE_FOR_BLOCK_META("blockId {0} is {1} bytes, but only {2} bytes available in {3}"),

  // tieredBlockStore
  BLOCK_ID_FOR_DIFFERENT_SESSION("BlockId {0} is owned by sessionId {1} not {2}"),
  BLOCK_NOT_FOUND_AT_LOCATION("Block {0} not found at location: {1}"),
  MOVE_UNCOMMITTED_BLOCK("Cannot move uncommitted block {0}"),
  NO_BLOCK_ID_FOUND("BlockId {0} not found"),
  NO_EVICTION_PLAN_TO_FREE_SPACE("No eviction plan by evictor to free space"),
  NO_SPACE_FOR_BLOCK_ALLOCATION("Failed to allocate {0} bytes after {1} retries for blockId {2}"),
  NO_SPACE_FOR_BLOCK_MOVE("Failed to find space in {0} to move blockId {1} after {2} retries"),
  REMOVE_UNCOMMITTED_BLOCK("Cannot remove uncommitted block {0}"),
  TEMP_BLOCK_ID_COMMITTED("Temp blockId {0} is not available, because it is already committed"),
  TEMP_BLOCK_ID_EXISTS("Temp blockId {0} is not available, because it already exists"),

  // journal
  JOURNAL_WRITE_AFTER_CLOSE("Cannot write entry after closing the stream"),
  NO_ENTRY_TYPE("Could not find entry type for journal entry. Unrecognized fields: {0}"),
  UNEXPECTED_JOURNAL_ENTRY("Unexpected entry in journal: {0}"),
  UNKNOWN_JOURNAL_ENTRY_TYPE("Unknown journal entry type: {0}"),

  // file
  CANNOT_READ_DIRECTORY("Cannot read from {0} because it is a directory"),
  DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE(
      "Cannot delete non-empty directory {0} because recursive is set to false"),
  FILE_ALREADY_EXISTS("{0} already exists"),
  FILE_CREATE_IS_DIRECTORY("{0} already exists. Directories cannot be overwritten with create"),
  HDFS_FILE_NOT_FOUND("File {0} with id {1} is not found"),

  // file system master
  PATH_MUST_HAVE_VALID_PARENT("{0} does not have a valid parent"),
  FILEID_MUST_BE_FILE("File id {0} must be a file"),
  RENAME_CANNOT_BE_ONTO_MOUNT_POINT("{0} is a mount point and cannot be renamed onto"),
  RENAME_CANNOT_BE_ACROSS_MOUNTS("Renaming {0} to {1} is a cross mount operation"),
  RENAME_CANNOT_BE_TO_ROOT("Cannot rename a path to the root directory"),
  RENAME_CANNOT_BE_TO_SUBDIRECTORY("Cannot rename because {0} is a prefix of {1}"),
  ROOT_CANNOT_BE_RENAMED("The root directory cannot be renamed"),

  // file system master ufs
  FAILED_UFS_CREATE("Failed to create {0} in the under file system"),
  FAILED_UFS_RENAME("Failed to rename {0} to {1} in the under file system"),

  // shell
  DESTINATION_FILE_CANNOT_EXIST_WITH_WILDCARD_SOURCE(
      "The destination cannot be an existent file when the src is a directory or contains"
            +  " wildcards."),

  // lineage
  DELETE_LINEAGE_WITH_CHILDREN("The lineage {0} to delete has child lineages"),
  LINEAGE_DOES_NOT_EXIST("The lineage {0} does not exist"),
  LINEAGE_INPUT_FILE_NOT_EXIST("The lineage input file {0} does not exist"),
  LINEAGE_OUTPUT_FILE_NOT_EXIST("No lineage has output file {0}"),
  MISSING_REINITIALIZE_FILE("Cannot reinitialize file {0} because its lineage does not exist"),
  UNKNOWN_LINEAGE_FILE_STATE("Unknown LineageFileState: {0}"),

  // client
  INCOMPATIBLE_VERSION("{0} client version {1} is not compatible with server version {2}"),

  // configuration
  DEFAULT_PROPERTIES_FILE_DOES_NOT_EXIST("The default Alluxio properties file does not exist"),
  INVALID_CONFIGURATION_KEY("Invalid configuration key {0}"),
  INVALID_CONFIGURATION_VALUE("Invalid value {0} for configuration key {1}"),
  KEY_NOT_BYTES("Configuration cannot evaluate key {0} as bytes"),
  KEY_NOT_DOUBLE("Configuration cannot evaluate key {0} as double"),
  KEY_NOT_INTEGER("Configuration cannot evaluate key {0} as integer"),
  UNABLE_TO_LOAD_PROPERTIES_FILE("Unable to load default Alluxio properties file"),
  UNKNOWN_PROPERTY("Unknown property for {0} {1}"),

  // security
  AUTHENTICATION_IS_NOT_ENABLED("Authentication is not enabled"),
  AUTHORIZED_CLIENT_USER_IS_NULL("The client user is not authorized so as to be null in server"),
  INVALID_SET_ACL_OPTIONS("Invalid set acl options: {0}, {1}, {2}"),
  PERMISSION_DENIED("Permission denied: {0}"),
  PERMISSION_IS_NULL("Permission cannot be null when constructing PermissionStatus"),
  SECURITY_IS_NOT_ENABLED("Security is not enabled"),

  // yarn
  YARN_NOT_ENOUGH_HOSTS(
      "Not enough usable hosts in the cluster to launch {0} workers. Only {1} hosts available"),

  // mounting
  MOUNT_POINT_ALREADY_EXISTS("Mount point {0} already exists"),
  MOUNT_POINT_PREFIX_OF_ANOTHER("Mount point {0} is a prefix of {1}"),
  UFS_PATH_DOES_NOT_EXIST("Ufs path {0} does not exist"),
  MOUNT_PATH_SHADOWS_DEFAULT_UFS(
      "Mount path {0} shadows an existing path in the default underlying filesystem"),
  MOUNT_READONLY("A write operation on {0} is under a readonly mount point {1}"),

  // key-value
  KEY_VALUE_TOO_LARGE("Unable to put key-value pair: key {0} bytes, value {1} bytes"),
  INVALID_KEY_VALUE_STORE_URI("The URI {0} exists but is not a key-value store"),
  KEY_ALREADY_EXISTS("The input key already exists in the key-value store"),

  // SEMICOLON! minimize merge conflicts by putting it on its own line
  ;

  private final MessageFormat mMessage;

  ExceptionMessage(String message) {
    mMessage = new MessageFormat(message);
  }

  /**
   * Formats the message of the exception.
   *
   * @param params the parameters for the exception message
   * @return the formatted message
   */
  public String getMessage(Object... params) {
    Preconditions.checkArgument(mMessage.getFormats().length == params.length, "The message takes "
        + mMessage.getFormats().length + " arguments, but is given " + params.length);
    // MessageFormat is not thread-safe, so guard it
    synchronized (mMessage) {
      return mMessage.format(params);
    }
  }
}
