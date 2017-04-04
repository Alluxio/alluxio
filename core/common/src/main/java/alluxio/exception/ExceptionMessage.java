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
  DEPENDENCY_CYCLE("Dependency cycle encountered"),
  INVALID_PREFIX("Parent path {0} is not a prefix of child {1}"),
  NOT_SUPPORTED("This method is not supported"),
  PATH_DOES_NOT_EXIST("Path {0} does not exist"),
  PATH_MUST_BE_FILE("Path {0} must be a file"),
  PATH_MUST_BE_DIRECTORY("Path {0} must be a directory"),
  PATH_INVALID("Path {0} is invalid"),
  RESOURCE_UNAVAILABLE("Resource unavailable"),

  // general block
  CANNOT_REQUEST_SPACE("Not enough space left on worker {0} to store blockId {1,number,#}."),
  NO_LOCAL_WORKER("Local address {0} requested but there is no local worker"),
  NO_WORKER_AVAILABLE_ON_ADDRESS("No Alluxio worker available for address {0}"),
  NO_WORKER_AVAILABLE("No available Alluxio worker found"),

  // block lock manager
  LOCK_ID_FOR_DIFFERENT_BLOCK("lockId {0,number,#} is for blockId {1,number,#}, not {2,number,#}"),
  LOCK_ID_FOR_DIFFERENT_SESSION(
      "lockId {0,number,#} is owned by sessionId {1,number,#} not {2,number,#}"),
  LOCK_RECORD_NOT_FOUND_FOR_BLOCK_AND_SESSION(
      "no lock is found for blockId {0,number,#} for sessionId {1,number,#}"),
  LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID("lockId {0,number,#} has no lock record"),

  // block metadata manager and view
  BLOCK_META_NOT_FOUND("BlockMeta not found for blockId {0,number,#}"),
  GET_DIR_FROM_NON_SPECIFIC_LOCATION("Cannot get path from non-specific dir {0}"),
  TEMP_BLOCK_META_NOT_FOUND("TempBlockMeta not found for blockId {0,number,#}"),
  TIER_ALIAS_NOT_FOUND("Tier with alias {0} not found"),
  TIER_VIEW_ALIAS_NOT_FOUND("Tier view with alias {0} not found"),

  // instream/outstream
  FAILED_CACHE("Failed to cache: {0}"),
  FAILED_CREATE("Failed to create {0}"),
  FAILED_SEEK("Failed to seek to {0}"),
  FAILED_SKIP("Failed to skip {0}"),
  INSTREAM_CANNOT_SKIP("The underlying BlockInStream could not skip {0}"),
  READ_CLOSED_STREAM("Cannot read from a closed stream"),
  SEEK_NEGATIVE("Seek position is negative: {0,number,#}"),
  SEEK_PAST_EOF("Seek position is past EOF: {0,number,#}, fileSize: {1,number,#}"),

  // netty
  BLOCK_WRITE_ERROR(
      "Error writing blockId: {0,number,#}, sessionId: {1,number,#}, address: {2}, message: {3}"),
  NO_RPC_HANDLER("No handler implementation for rpc message type: {0}"),
  UNDER_FILE_WRITE_ERROR(
          "Error writing to under file system fileId: {0,number,#}, addr: {1}, msg: {2}"),
  UNEXPECTED_RPC_RESPONSE("Unexpected response message type: {0} (expected: {1})"),
  WRITER_ALREADY_OPEN(
      "This writer is already open for address: {0}, blockId: {1,number,#}, "
          + "sessionId: {2,number,#}"),

  // storageDir
  ADD_EXISTING_BLOCK("blockId {0,number,#} exists in {1}"),
  BLOCK_NOT_FOUND_FOR_SESSION("blockId {0,number,#} in {1} not found for session {2,number,#}"),
  NO_SPACE_FOR_BLOCK_META(
      "blockId {0,number,#} is {1,number,#} bytes, but only {2,number,#} bytes available in {3}"),

  // tieredBlockStore
  BLOCK_ID_FOR_DIFFERENT_SESSION(
      "blockId {0,number,#} is owned by sessionId {1,number,#} not {2,number,#}"),
  BLOCK_NOT_FOUND_AT_LOCATION("blockId {0,number,#} not found at location: {1}"),
  MOVE_UNCOMMITTED_BLOCK("Cannot move uncommitted blockId {0,number,#}"),
  NO_BLOCK_ID_FOUND("blockId {0,number,#} not found"),
  NO_EVICTION_PLAN_TO_FREE_SPACE("No eviction plan by evictor to free space"),
  NO_SPACE_FOR_BLOCK_ALLOCATION(
      "Failed to allocate {0,number,#} bytes after {1} retries for blockId {2,number,#}"),
  NO_SPACE_FOR_BLOCK_MOVE(
      "Failed to find space in {0} to move blockId {1,number,#} after {2} retries"),
  REMOVE_UNCOMMITTED_BLOCK("Cannot remove uncommitted blockId {0,number,#}"),
  TEMP_BLOCK_ID_COMMITTED(
      "Temp blockId {0,number,#} is not available, because it is already committed"),
  TEMP_BLOCK_ID_EXISTS("Temp blockId {0,number,#} is not available, because it already exists"),

  // ufsBlockStore
  UFS_BLOCK_ALREADY_EXISTS_FOR_SESSION(
      "UFS block {0,number,#} from UFS file {1} exists for session {2,number,#}"),
  UFS_BLOCK_ACCESS_TOKEN_UNAVAILABLE(
      "Failed to acquire an access token for the UFS block {0,number,#} (filename: {1})."),
  UFS_BLOCK_DOES_NOT_EXIST_FOR_SESSION(
      "UFS block {0,number,#} does not exist for session {1,number,#}"),

  // journal
  JOURNAL_WRITE_AFTER_CLOSE("Cannot write entry after closing the stream"),
  JOURNAL_WRITE_FAILURE("Failed to write to journal file ({0}): {1}"),
  JOURNAL_FLUSH_FAILURE("Failed to flush journal file ({0}): {1}"),
  UNEXPECTED_JOURNAL_ENTRY("Unexpected entry in journal: {0}"),

  // file
  CANNOT_READ_DIRECTORY("Cannot read from {0} because it is a directory"),
  DELETE_FAILED_UFS("Failed to delete {0} from the under file system"),
  DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE(
      "Cannot delete non-empty directory {0} because recursive is set to false"),
  DELETE_ROOT_DIRECTORY("Cannot delete the root directory"),
  FILE_ALREADY_EXISTS("{0} already exists"),
  FILE_CREATE_IS_DIRECTORY("{0} already exists. Directories cannot be overwritten with create"),
  HDFS_FILE_NOT_FOUND("File {0} with URI {1} is not found"),
  PARENT_CREATION_FAILED("Unable to create parent directories for path {0}"),

  // file system master
  CANNOT_FREE_NON_EMPTY_DIR("Cannot free directory {0} which is not empty. Please set "
      + "the \"recursive\" flag of free operation to true"),
  CANNOT_FREE_NON_PERSISTED_FILE("Cannot free file {0} which is not persisted"),
  CANNOT_FREE_PINNED_FILE("Cannot free file {0} which is pinned. Please unpin it first or"
      + " set the \"forced\" flag of free operation to true"),
  INODE_DOES_NOT_EXIST("inodeId {0,number,#} does not exist"),
  INODE_DOES_NOT_EXIST_RETRIES("inodeId {0,number,#} does not exist; too many retries"),
  NOT_MUTABLE_INODE_PATH("Not a MutableLockedInodePath: {0}"),
  PATH_COMPONENTS_INVALID("Parameter pathComponents is {0}"),
  PATH_COMPONENTS_INVALID_START("Path starts with {0}"),
  PATH_INVALID_CONCURRENT_RENAME("Path is no longer valid, possibly due to a concurrent rename."),
  PATH_MUST_HAVE_VALID_PARENT("{0} does not have a valid parent"),
  RENAME_CANNOT_BE_ACROSS_MOUNTS("Renaming {0} to {1} is a cross mount operation"),
  RENAME_CANNOT_BE_ONTO_MOUNT_POINT("{0} is a mount point and cannot be renamed onto"),
  RENAME_CANNOT_BE_TO_ROOT("Cannot rename a path to the root directory"),
  RENAME_CANNOT_BE_TO_SUBDIRECTORY("Cannot rename because {0} is a prefix of {1}"),
  ROOT_CANNOT_BE_RENAMED("The root directory cannot be renamed"),

  // block master
  NO_WORKER_FOUND("No worker with workerId {0,number,#} is found"),

  // file system master ufs
  FAILED_UFS_CREATE("Failed to create {0} in the under file system"),
  FAILED_UFS_RENAME("Failed to rename {0} to {1} in the under file system"),

  // file system worker
  BAD_WORKER_FILE_ID(
      "Worker fileId {0,number,#} is invalid. The worker may have crashed or cleaned up "
          + "the client state due to a timeout."),

  // shell
  DESTINATION_CANNOT_BE_FILE(
      "The destination cannot be an existing file when the source is a directory or a list of "
          + "files."),

  // lineage
  DELETE_LINEAGE_WITH_CHILDREN("The lineage {0} to delete has child lineages"),
  LINEAGE_DOES_NOT_EXIST("The lineage {0} does not exist"),
  LINEAGE_INPUT_FILE_NOT_EXIST("The lineage input file {0} does not exist"),
  LINEAGE_OUTPUT_FILE_NOT_EXIST("No lineage has output file {0}"),
  MISSING_REINITIALIZE_FILE("Cannot reinitialize file {0} because its lineage does not exist"),
  UNKNOWN_LINEAGE_FILE_STATE("Unknown LineageFileState: {0}"),

  // client
  DIFFERENT_MASTER_ADDRESS("Master address {0} is different from that in file system context {1}"),
  INCOMPATIBLE_VERSION("{0} client version {1} is not compatible with server version {2}"),

  // configuration
  DEFAULT_PROPERTIES_FILE_DOES_NOT_EXIST("The default Alluxio properties file does not exist"),
  INVALID_CONFIGURATION_VALUE("Invalid value {0} for configuration key {1}"),
  KEY_NOT_BOOLEAN("Configuration cannot evaluate key {0} as boolean"),
  KEY_NOT_BYTES("Configuration cannot evaluate key {0} as bytes"),
  KEY_NOT_DOUBLE("Configuration cannot evaluate key {0} as double"),
  KEY_NOT_FLOAT("Configuration cannot evaluate key {0} as float"),
  KEY_NOT_INTEGER("Configuration cannot evaluate key {0} as integer"),
  KEY_NOT_LONG("Configuration cannot evaluate key {0} as long"),
  UNDEFINED_CONFIGURATION_KEY("No value set for configuration key {0}"),
  UNKNOWN_PROPERTY("Unknown property for {0} {1}"),

  // security
  AUTHENTICATION_IS_NOT_ENABLED("Authentication is not enabled"),
  AUTHORIZED_CLIENT_USER_IS_NULL("The client user is not authorized so as to be null in server"),
  INVALID_SET_ACL_OPTIONS("Invalid set acl options: {0}, {1}, {2}"),
  INVALID_MODE("Invalid mode {0}"),
  INVALID_MODE_SEGMENT("Invalid mode {0} - contains invalid segment {1}"),
  INVALID_MODE_PERMISSIONS(
      "Invalid mode {0} - contains invalid segment {1} which has invalid permissions {2}"),
  INVALID_MODE_TARGETS(
      "Invalid mode {0} - contains invalid segment {1} which has invalid targets {2}"),
  PERMISSION_DENIED("Permission denied: {0}"),
  SECURITY_IS_NOT_ENABLED("Security is not enabled"),

  // yarn
  YARN_NOT_ENOUGH_HOSTS(
      "Not enough usable hosts in the cluster to launch {0} {1} containers. Only {2} hosts "
          + "available"),
  YARN_NOT_ENOUGH_RESOURCES(
      "{0} {1} specified above max threshold of cluster, specified={2}, max={3}"),

  // mounting
  MOUNT_POINT_ALREADY_EXISTS("Mount point {0} already exists"),
  MOUNT_POINT_PREFIX_OF_ANOTHER("Mount point {0} is a prefix of {1}"),
  MOUNT_PATH_SHADOWS_DEFAULT_UFS(
      "Mount path {0} shadows an existing path in the default underlying filesystem"),
  MOUNT_READONLY("A write operation on {0} is under a readonly mount point {1}"),
  UFS_PATH_DOES_NOT_EXIST("Ufs path {0} does not exist"),

  // key-value
  KEY_VALUE_TOO_LARGE("Unable to put key-value pair: key {0} bytes, value {1} bytes"),
  KEY_ALREADY_EXISTS("The input key already exists in the key-value store"),
  INVALID_KEY_VALUE_STORE_URI("The URI {0} exists but is not a key-value store"),

  // block worker
  FAILED_COMMIT_BLOCK_TO_MASTER("Failed to commit block with blockId {0,number,#} to master"),

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
    Preconditions.checkArgument(mMessage.getFormats().length == params.length,
        "The message takes " + mMessage.getFormats().length + " arguments, but is given "
            + params.length);
    // MessageFormat is not thread-safe, so guard it
    synchronized (mMessage) {
      return mMessage.format(params);
    }
  }

  /**
   * Formats the message of the exception with a url to consult.
   *
   * @param url the url to consult
   * @param params the parameters for the exception message
   * @return the formatted message
   */
  public String getMessageWithUrl(String url, Object... params) {
    return getMessage(params) + " Please consult " + url
        + " for common solutions to address this problem.";
  }
}
