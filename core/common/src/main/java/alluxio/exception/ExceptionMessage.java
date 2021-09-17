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

import alluxio.Constants;

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
  INVALID_PREFIX("Parent path \"{0}\" is not a prefix of child {1}."),
  NOT_SUPPORTED("This method is not supported."),
  PATH_DOES_NOT_EXIST("Path \"{0}\" does not exist."),
  PATH_MUST_BE_FILE("Path \"{0}\" must be a file."),
  PATH_MUST_BE_MOUNT_POINT("Path \"{0}\" must be a mount point."),
  PATH_INVALID("Path \"{0}\" is invalid."),
  STATE_LOCK_TIMED_OUT("Failed to acquire the lock after {0}ms"),

  // general block
  BLOCK_UNAVAILABLE("Block {0} is unavailable in both Alluxio and UFS."),
  BLOCK_SIZE_INVALID("Block size of {0} is invalid. Block size must be > 0 bytes."),
  CANNOT_REQUEST_SPACE("Not enough space left on worker {0} to store blockId {1,number,#}."),
  NO_SPACE_FOR_BLOCK_ON_WORKER("There is no worker with enough space for a new block of size {0}"),
  NO_WORKER_AVAILABLE("No available Alluxio worker found"),

  // active sync
  FAILED_INITIAL_SYNC("IOException encountered during initial syncing of sync point {0}"),

  // block lock manager
  LOCK_ID_FOR_DIFFERENT_BLOCK("lockId {0,number,#} is for blockId {1,number,#}, not {2,number,#}"),
  LOCK_ID_FOR_DIFFERENT_SESSION(
      "lockId {0,number,#} is owned by sessionId {1,number,#} not {2,number,#}"),
  LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID("lockId {0,number,#} has no lock record"),
  LOCK_NOT_RELEASED("lockId {0,number,#} is not released."),
  SESSION_NOT_CLOSED("session {0,number,#} is not closed."),

  // block metadata manager and view
  BLOCK_META_NOT_FOUND("BlockMeta not found for blockId {0,number,#}"),
  GET_DIR_FROM_NON_SPECIFIC_LOCATION("Cannot get path from non-specific dir {0}"),
  TEMP_BLOCK_META_NOT_FOUND("TempBlockMeta not found for blockId {0,number,#}"),
  TIER_ALIAS_NOT_FOUND("Tier with alias {0} not found"),
  TIER_VIEW_ALIAS_NOT_FOUND("Tier view with alias {0} not found"),

  // instream/outstream
  FAILED_CACHE("Failed to cache: {0}"),
  READ_CLOSED_STREAM("Cannot read from a closed stream"),

  // meta master
  NO_MASTER_FOUND("No master with masterId {0,number,#} is found"),

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
  NO_EVICTION_PLAN_TO_FREE_SPACE(
      "Failed to free {0,number,#} bytes space at location {1}"),
  NO_SPACE_FOR_BLOCK_ALLOCATION(
      "Failed to allocate {0,number,#} bytes on {1} to create blockId {2,number,#}"),
  NO_SPACE_FOR_BLOCK_MOVE(
      "Failed to find space in {0} to move blockId {1,number,#}"),
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

  // Raft journal
  FAILED_RAFT_BOOTSTRAP("Failed to bootstrap raft cluster with addresses {0}: {1}"),

  // file
  CANNOT_READ_INCOMPLETE_FILE(
      "Cannot read from {0} because it is incomplete. Wait for the file to be marked as complete "
          + "by the writing thread or application."),
  CANNOT_READ_DIRECTORY("Cannot read from {0} because it is a directory"),
  DELETE_FAILED_DIR_CHILDREN(
      "Cannot delete directory {0}. Failed to delete children: {1}"),
  DELETE_FAILED_DIR_NONEMPTY("Directory not empty"),
  DELETE_FAILED_UFS("Failed to delete {0} from the under file system"),
  DELETE_FAILED_UFS_DIR("UFS delete dir failed"),
  DELETE_FAILED_UFS_FILE("UFS delete file failed"),
  DELETE_FAILED_UFS_NOT_IN_SYNC("UFS dir not in sync. Sync UFS, or delete with unchecked flag."),
  DELETE_FAILED_DIRECTORY_NOT_IN_SYNC(
      "Cannot delete {0} because the UFS has contents not loaded into Alluxio. Sync Alluxio with "
          + "UFS or run delete with unchecked flag to forcibly delete"),
  DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE(
      "Cannot delete non-empty directory {0} because recursive is set to false"),
  DELETE_ROOT_DIRECTORY("Cannot delete the root directory"),
  FILE_CREATE_IS_DIRECTORY("{0} already exists. Directories cannot be overwritten with create"),
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
  PATH_INVALID_CONCURRENT_DELETE("Path is no longer valid, possibly due to a concurrent delete."),
  PATH_MUST_HAVE_VALID_PARENT("{0} does not have a valid parent"),
  RENAME_CANNOT_BE_ACROSS_MOUNTS("Renaming {0} to {1} is a cross mount operation"),
  RENAME_CANNOT_BE_ONTO_MOUNT_POINT("{0} is a mount point and cannot be renamed onto"),
  RENAME_CANNOT_BE_TO_ROOT("Cannot rename a path to the root directory"),
  RENAME_CANNOT_BE_TO_SUBDIRECTORY("Cannot rename because {0} is a prefix of {1}"),
  ROOT_CANNOT_BE_RENAMED("The root directory cannot be renamed"),
  JOURNAL_ENTRY_MISSING(
      "Journal entries are missing between sequence number {0} (inclusive) and {1} (exclusive)."),
  JOURNAL_ENTRY_TRUNCATED_UNEXPECTEDLY(
      "Journal entry [sequence number {0}] is truncated unexpectedly."),

  // block master
  NO_WORKER_FOUND("No worker with workerId {0,number,#} is found"),

  // table master
  DATABASE_DOES_NOT_EXIST("Database {0} does not exist"),
  TABLE_DOES_NOT_EXIST("Table {0} does not exist in database {1}"),
  TRANSFORM_WRITE_ACTION_INVALID_NUM_FILES("Write action must have positive number of files"),
  TABLE_BEING_TRANSFORMED("Existing job {0} is transforming table {1} in database {2}"),
  TABLE_ALREADY_TRANSFORMED("Database {0} table {1} has been transformed by definition {2}"),
  TRANSFORM_JOB_DOES_NOT_EXIST("No transformation information for job ID {0}"),
  TRANSFORM_MANAGER_HEARTBEAT_INTERRUPTED("TransformManager's heartbeat was interrupted"),
  TRANSFORM_JOB_ID_NOT_FOUND_IN_JOB_SERVICE("Transformation job {0} for database {1} table {2} "
      + "was not found in job master, maybe the job master was restarted: {3}"),

  // safe mode
  MASTER_IN_SAFEMODE("Alluxio master is in safe mode. Please try again later."),

  // file system master ufs
  FAILED_UFS_CREATE("Failed to create {0} in the under file system"),
  FAILED_UFS_RENAME("Failed to rename {0} to {1} in the under file system"),

  // file system worker
  BAD_WORKER_FILE_ID(
      "Worker fileId {0,number,#} is invalid. The worker may have crashed or cleaned up "
          + "the client state due to a timeout."),

  // cli
  INVALID_ARGS_GENERIC("Invalid args for command {0}"),
  INVALID_ARGS_NULL("Null args for command {0}"),
  INVALID_ARGS_NUM("Command {0} takes {1} arguments, not {2}"),
  INVALID_ARGS_NUM_INSUFFICIENT("Command {0} requires at least {1} arguments ({2} provided)"),
  INVALID_ARGS_NUM_TOO_MANY("Command {0} requires at most {1} arguments ({2} provided)"),
  INVALID_ARGS_SORT_FIELD("Invalid sort option `{0}` for --sort"),
  INVALID_ARG_TYPE("Arg {0} is not type {1}"),
  INVALID_OPTION("Invalid option provided. Supported options: {0}"),
  INVALID_OPTION_COUNT("Invalid option count. Expected: {0}, Found: {1}"),
  INVALID_OPTION_VALUE("Invalid value provided for option: {0}. Supported values: {1}"),
  INVALID_ADDRESS_VALUE("Invalid address provided."),

  // extension shell
  INVALID_EXTENSION_NOT_JAR("File {0} does not have the extension JAR"),

  // fs shell
  DESTINATION_CANNOT_BE_FILE(
      "The destination cannot be an existing file when the source is a directory or a list of "
          + "files."),
  INVALID_TIME("{0} is not valid time"),

  // client
  DIFFERENT_CONNECTION_DETAILS(
      "New connection details are different from that in file system context {0}"),
  INCOMPATIBLE_VERSION("{0} client version {1} is not compatible with server version {2}"),

  // configuration
  UNABLE_TO_DETERMINE_MASTER_HOSTNAME("Cannot run {0}; Unable to determine {1} address. Please "
      + "modify " + Constants.SITE_PROPERTIES + " to either set {2}, configure zookeeper with "
      + "{3}=true and {4}=[comma-separated zookeeper master addresses], or utilize internal HA by "
      + "setting {5}=[comma-separated alluxio {1} addresses]"),
  DEFAULT_PROPERTIES_FILE_DOES_NOT_EXIST("The default Alluxio properties file does not exist"),
  INVALID_CONFIGURATION_KEY("Invalid property key {0}"),
  INVALID_CONFIGURATION_VALUE("Invalid value {0} for configuration key {1}"),
  KEY_NOT_BOOLEAN("Configuration cannot evaluate value {0} as boolean for key {1}"),
  KEY_NOT_BYTES("Configuration cannot evaluate value {0} as bytes for key {1}"),
  KEY_NOT_DOUBLE("Configuration cannot evaluate value {0} as double for key {1}"),
  KEY_NOT_FLOAT("Configuration cannot evaluate value {0} as float for key {1}"),
  KEY_NOT_INTEGER("Configuration cannot evaluate value {0} as integer for key {1}"),
  KEY_NOT_LONG("Configuration cannot evaluate value {0} as long for key {1}"),
  KEY_NOT_MS("Configuration cannot evaluate value {0} as milliseconds for key {1}"),
  KEY_CIRCULAR_DEPENDENCY("Circular dependency found while resolving {0}"),
  UNDEFINED_CONFIGURATION_KEY("No value set for configuration key {0}"),
  UNKNOWN_ENUM("Unrecognized configuration enum value <{0}> for key {1}. Acceptable values: {2}"),
  UNKNOWN_PROPERTY("Unknown property for {0} {1}"),

  // metrics
  INVALID_METRIC_KEY("Invalid metric key {0}"),

  // security
  ACL_BASE_REQUIRED(
      "Replacing ACL entries must include the base entries for 'user', 'group', and 'other'. "
          + "missing: {0}"),
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
  MOUNT_POINT_ALREADY_EXISTS("Mount point target path {0} already exists in Alluxio namespace. "
      + "Try mounting to another path."),
  MOUNT_POINT_PREFIX_OF_ANOTHER("Mount point {0} is a prefix of {1}"),
  MOUNT_PATH_SHADOWS_PARENT_UFS(
      "Mount path {0} shadows an existing path {1} in the parent underlying filesystem"),
  MOUNT_READONLY("A write operation on {0} under a readonly mount point {1} is not allowed"),
  UFS_PATH_DOES_NOT_EXIST("Ufs path {0} does not exist"),
  FOREIGN_URI_NOT_MOUNTED("Foreign URI: {0} is not found on Alluxio mounts."),

  // key-value
  KEY_VALUE_TOO_LARGE("Unable to put key-value pair: key {0} bytes, value {1} bytes"),
  KEY_ALREADY_EXISTS("The input key already exists in the key-value store"),
  INVALID_KEY_VALUE_STORE_URI("The URI {0} exists but is not a key-value store"),

  // migrate job (either move or copy)
  MIGRATE_CANNOT_BE_TO_SUBDIRECTORY("Cannot migrate because {0} is a prefix of {1}"),
  MIGRATE_DIRECTORY("Cannot migrate directory"),
  MIGRATE_FILE_TO_DIRECTORY("Cannot migrate a file ({0}) to a directory ({1})"),
  MIGRATE_NEED_OVERWRITE("Cannot migrate to {0} because it exists and overwrite is set to false"),
  MIGRATE_OVERWRITE_DIRECTORY(
      "{0} already exists. The overwrite flag cannot be used to overwrite directories"),
  MIGRATE_TO_FILE_AS_DIRECTORY("Cannot migrate to {0}. {1} is a file, not a directory"),

  // transform job
  TRANSFORM_TABLE_URI_LACKS_SCHEME("URI {0} lacks scheme"),
  TRANSFORM_TABLE_URI_LACKS_AUTHORITY("URI {0} lacks authority"),

  // job service
  NO_LOCAL_BLOCK_WORKER_REPLICATE_TASK(
      "Cannot find a local block worker to replicate blockId {0,number,#}"),

  // job manager
  JOB_DEFINITION_DOES_NOT_EXIST("The job definition for config {0} does not exist"),
  JOB_DOES_NOT_EXIST("The job of id {0} does not exist"),
  JOB_MASTER_FULL_CAPACITY("Job master is at full capacity of {0} jobs"),

  // block worker
  FAILED_COMMIT_BLOCK_TO_MASTER("Failed to commit block with blockId {0,number,#} to master"),
  PINNED_TO_MULTIPLE_MEDIUMTYPES("File {0} pinned to multiple medium types"),

  // ufs maintenance
  UFS_OP_NOT_ALLOWED("Operation {0} not allowed on ufs path {1} under maintenance mode {2}"),

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
    Preconditions.checkArgument(mMessage.getFormatsByArgumentIndex().length == params.length,
        "The message takes " + mMessage.getFormatsByArgumentIndex().length + " arguments, but is "
            + "given " + params.length);
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
