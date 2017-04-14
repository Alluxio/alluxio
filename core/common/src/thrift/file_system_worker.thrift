namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct CancelUfsFileTOptions {
}

struct CloseUfsFileTOptions {
}

struct CompleteUfsFileTOptions {
  1: optional string owner
  2: optional string group
  3: optional i16 mode
}

struct CreateUfsFileTOptions {
  1: optional string owner
  2: optional string group
  3: optional i16 mode
}

struct OpenUfsFileTOptions {
}

/**
 * This interface contains file system worker service endpoints for Alluxio clients.
 */
service FileSystemWorkerClientService extends common.AlluxioService {
  /**
   * Cancels a file which has not been completed in the under file system.
   */
  void cancelUfsFile(
    /** the id of the current session */ 1: i64 sessionId,
    /** the worker specific file id of the ufs file */ 2: i64 tempUfsFileId,
    /** the options for canceling the file */ 3: CancelUfsFileTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Closes a file in the under file system which was previously opened for reading.
   **/
  void closeUfsFile(
    /** the id of the current session */ 1: i64 sessionId,
    /** the worker specific file id of the ufs file */ 2: i64 tempUfsFileId,
    /** the options for closing the file */ 3: CloseUfsFileTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Completes a file in the under file system.
   */
  i64 completeUfsFile(
    /** the id of the current session */ 1: i64 sessionId,
    /** the worker specific file id of the ufs file */ 2: i64 tempUfsFileId,
    /** the options for completing the file */ 3: CompleteUfsFileTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Creates a file in the under file system.
   */
  i64 createUfsFile(
    /** the id of the current session */ 1: i64 sessionId,
    /** the path of the file in the ufs */ 2: string ufsPath,
    /** the options for creating the file */ 3: CreateUfsFileTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Opens an existing file in the under file system for reading.
   */
  i64 openUfsFile(
    /** the id of the current session */ 1: i64 sessionId,
    /** the path of the file in the ufs */ 2: string ufsPath,
    /** the options for opening the file */ 3: OpenUfsFileTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Local session send heartbeat to local worker to keep its state.
   */
  void sessionHeartbeat(
    /** the id of the current session */ 1: i64 sessionId,
    /** the client metrics. deprecated since 1.3.0 and will be removed in 2.0 */ 2: list<i64> metrics,
    )
    throws (1: exception.AlluxioTException e)
}
