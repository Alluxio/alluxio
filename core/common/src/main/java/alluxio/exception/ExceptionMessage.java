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
  PATH_DOES_NOT_EXIST("Path \"{0}\" does not exist."),
  BUCKET_DOES_NOT_EXIST("Bucket \"{0}\" does not exist."),
  PATH_DOES_NOT_EXIST_PARTIAL_LISTING("Path \"{0}\" was removed during listing."),
  INODE_NOT_FOUND_PARTIAL_LISTING("\"{0}\" Inode was not found during partial listing. It was "
      + "likely removed across listing calls."),
  INODE_NOT_IN_PARTIAL_LISTING("Inode not found in root path \"{0}\" during partial listing. "
      + "It was likely moved across listing calls."),
  PATH_MUST_BE_FILE("Path \"{0}\" must be a file."),
  PATH_INVALID("Path \"{0}\" is invalid."),
  STATE_LOCK_TIMED_OUT("Failed to acquire the lock after {0}ms"),

  // general block
  CANNOT_REQUEST_SPACE("Not enough space left on worker {0} to store blockId {1,number,#}."),
  NO_SPACE_FOR_BLOCK_ON_WORKER("There is no worker with enough space for a new block of size {0}"),
  NO_WORKER_AVAILABLE("No available Alluxio worker found"),

  // block metadata manager and view
  BLOCK_META_NOT_FOUND("BlockMeta not found for blockId {0,number,#}"),
  TEMP_BLOCK_META_NOT_FOUND("TempBlockMeta not found for blockId {0,number,#}"),
  TIER_ALIAS_NOT_FOUND("Tier with alias {0} not found"),
  TIER_VIEW_ALIAS_NOT_FOUND("Tier view with alias {0} not found"),

  // instream/outstream
  FAILED_CACHE("Failed to cache: {0}"),
  READ_CLOSED_STREAM("Cannot read from a closed stream"),

  // storageDir
  ADD_EXISTING_BLOCK("blockId {0,number,#} exists in {1}"),
  BLOCK_NOT_FOUND_FOR_SESSION("blockId {0,number,#} in {1} not found for session {2,number,#}"),
  NO_SPACE_FOR_BLOCK_META(
      "blockId {0,number,#} is {1,number,#} bytes, but only {2,number,#} bytes available in {3}"),

  // tieredBlockStore
  BLOCK_ID_FOR_DIFFERENT_SESSION(
      "blockId {0,number,#} is owned by sessionId {1,number,#} not {2,number,#}"),
  NO_EVICTION_PLAN_TO_FREE_SPACE(
      "Failed to free {0,number,#} bytes space at location {1}"),
  NO_SPACE_FOR_BLOCK_MOVE(
      "Failed to find space in {0} to move blockId {1,number,#}"),
  REMOVE_UNCOMMITTED_BLOCK("Cannot remove uncommitted blockId {0,number,#}"),
  TEMP_BLOCK_ID_COMMITTED(
      "Temp blockId {0,number,#} is not available, because it is already committed"),

  // journal
  JOURNAL_WRITE_AFTER_CLOSE("Cannot write entry after closing the stream"),
  JOURNAL_WRITE_FAILURE("Failed to write to journal file ({0}): {1}"),
  JOURNAL_FLUSH_FAILURE("Failed to flush journal file ({0}): {1}"),

  // file
  DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE(
      "Cannot delete non-empty directory {0} because recursive is set to false"),
  DELETE_ROOT_DIRECTORY("Cannot delete the root directory"),
  PARENT_CREATION_FAILED("Unable to create parent directories for path {0}"),

  // file system master
  CANNOT_FREE_NON_EMPTY_DIR("Cannot free directory {0} which is not empty. Please set "
      + "the \"recursive\" flag of free operation to true"),
  CANNOT_FREE_NON_PERSISTED_FILE("Cannot free file {0} which is not persisted"),
  CANNOT_FREE_PINNED_FILE("Cannot free file {0} which is pinned. Please unpin it first or"
      + " set the \"forced\" flag of free operation to true"),
  INODE_DOES_NOT_EXIST("inodeId {0,number,#} does not exist"),
  START_AFTER_DOES_NOT_MATCH_PATH(
      "The start after partial listing option {0} is not a prefix of the listing path {1}"),
  PREFIX_DOES_NOT_MATCH_PATH(
      "Prefix component {0} does not match path component {1} of the listing offset"),
  PATH_MUST_HAVE_VALID_PARENT("{0} does not have a valid parent"),
  RENAME_CANNOT_BE_TO_ROOT("Cannot rename a path to the root directory"),
  ROOT_CANNOT_BE_RENAMED("The root directory cannot be renamed"),
  JOURNAL_ENTRY_MISSING(
      "Journal entries are missing between sequence number {0} (inclusive) and {1} (exclusive)."),
  CANNOT_OVERWRITE_DIRECTORY("{0} already exists. Directories cannot be overwritten with create"),
  CANNOT_OVERWRITE_FILE_WITHOUT_OVERWRITE("{0} already exists. If you want to overwrite the file,"
      + " you need to specify the overwrite option."),

  // block master
  NO_WORKER_FOUND("No worker with workerId {0,number,#} is found"),

  // table master
  DATABASE_DOES_NOT_EXIST("Database {0} does not exist"),
  TABLE_DOES_NOT_EXIST("Table {0} does not exist in database {1}"),
  TABLE_BEING_TRANSFORMED("Existing job {0} is transforming table {1} in database {2}"),
  TABLE_ALREADY_TRANSFORMED("Database {0} table {1} has been transformed by definition {2}"),
  TRANSFORM_JOB_DOES_NOT_EXIST("No transformation information for job ID {0}"),
  TRANSFORM_JOB_ID_NOT_FOUND_IN_JOB_SERVICE("Transformation job {0} for database {1} table {2} "
      + "was not found in job master, maybe the job master was restarted: {3}"),

  // safe mode
  MASTER_IN_SAFEMODE("Alluxio master is in safe mode. Please try again later."),

  // file system master ufs
  FAILED_UFS_RENAME("Failed to rename {0} to {1} in the under file system"),

  // cli
  INVALID_ARGS_NULL("Null args for command {0}"),
  INVALID_ARGS_NUM("Command {0} takes {1} arguments, not {2}"),
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

  // client
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

  // security
  AUTHENTICATION_IS_NOT_ENABLED("Authentication is not enabled"),
  INVALID_MODE("Invalid mode {0}"),
  INVALID_MODE_SEGMENT("Invalid mode {0} - contains invalid segment {1}"),
  PERMISSION_DENIED("Permission denied: {0}"),

  // mounting
  MOUNT_POINT_ALREADY_EXISTS("Mount point target path {0} already exists in Alluxio namespace. "
      + "Try mounting to another path."),
  MOUNT_POINT_PREFIX_OF_ANOTHER("Mount point {0} is a prefix of {1}"),
  MOUNT_READONLY("A write operation on {0} under a readonly mount point {1} is not allowed"),

  // migrate job (either move or copy)
  MIGRATE_CANNOT_BE_TO_SUBDIRECTORY("Cannot migrate because {0} is a prefix of {1}"),

  // transform job
  TRANSFORM_TABLE_URI_LACKS_SCHEME("URI {0} lacks scheme"),
  TRANSFORM_TABLE_URI_LACKS_AUTHORITY("URI {0} lacks authority"),

  // job service
  NO_LOCAL_BLOCK_WORKER_LOAD_TASK(
      "Cannot find a local block worker to load blockId {0,number,#}"),

  // job manager
  JOB_DEFINITION_DOES_NOT_EXIST("The job definition for config {0} does not exist"),
  JOB_DOES_NOT_EXIST("The job of id {0} does not exist"),
  JOB_MASTER_FULL_CAPACITY("Job master is at full capacity of {0} jobs"),

  // block worker
  FAILED_COMMIT_BLOCK_TO_MASTER("Failed to commit block with blockId {0,number,#} to master"),

  // ufs maintenance
  UFS_OP_NOT_ALLOWED("Operation {0} not allowed on ufs path {1} under maintenance mode {2}"),

  // RocksDB
  ROCKS_DB_CLOSING("RocksDB is being closed because the master is under one of the following "
      + "events: primary failover/shut down/checkpoint/journal replay"),
  ROCKS_DB_REWRITTEN("RocksDB has been rewritten. Typically this is because the master is "
      + "restored to a checkpoint."),
  ROCKS_DB_EXCLUSIVE_LOCK_FORCED("RocksDB exclusive lock is forced with {0} ongoing "
      + "r/w operations. There is a risk to crash!"),
  ROCKS_DB_REF_COUNT_DIRTY("Some read/write operations did not respect the exclusive lock on "
      + "the RocksStore and messed up the ref count! Current ref count is {0}."),

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
   * Formats the message of the exception with an url to consult.
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
