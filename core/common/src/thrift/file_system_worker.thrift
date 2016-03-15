namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct UFSCancelFileTOptions {
}

struct UFSCompleteFileTOptions {
}

struct UFSCreateFileTOptions {
}

service FileSystemWorkerClientService extends common.AlluxioService {
  /**
   * Cancels a file which has not been completed in the under file system.
   */
  void ufsCancelFile( /** the path of the file in the ufs */ 1: i64 workerFileId,
      /** the options for canceling the file */ 2: UFSCancelFileTOptions options)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Completes a file in the under file system.
   */
  void ufsCompleteFile( /** the path of the file in the ufs */ 1: i64 workerFileId,
      /** the options for completing the file */ 2: UFSCompleteFileTOptions options)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Creates a file in the under file system.
   */
  i64 ufsCreateFile( /** the path of the file in the ufs */ 1: string ufsPath,
      /** the options for creating the file */ 2: UFSCreateFileTOptions options)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)
}
