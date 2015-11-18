namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

struct CommandLineJobInfo {
  1: string command
  2: JobConfInfo conf
}

struct CheckpointFile {
  1: i64 fileId
  2: list<i64> blockIds
}

struct DependencyInfo{} // deprecated

struct JobConfInfo {
  1: string outputFile
}

struct LineageCommand {
  1: common.CommandType commandType
  2: list<CheckpointFile> checkpointFiles
}

struct LineageFileInfo {
  1: i64 id
  2: string state
  3: string underFilePath
}

struct LineageInfo {
  1: i64 id
  2: list<i64> inputFiles
  3: list<LineageFileInfo> outputFiles
  4: CommandLineJobInfo job
  5: i64 creationTimeMs
  6: list<i64> parents
  7: list<i64> children
}

service LineageMasterService extends common.TachyonService {

  /**
   * Marks a file as completed and initiates asynchronous persistence (if applicable).
   * @param fileId
   * @throws TachyonTException
   */
  void asyncCompleteFile(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /**
   * Creates a lineage.
   * @param inputFiles
   * @param outputFiles
   * @param job
   * @return lineageId
   * @throws TachyonTException
   * @throws ThriftIOException
   */
  i64 createLineage(1: list<string> inputFiles, 2: list<string> outputFiles, 3: CommandLineJobInfo job)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Deletes a lineage.
   * @param linearId
   * @param cascade
   * @return whether deletion succeeded
   * @throws TachyonTException
   */
  bool deleteLineage(1: i64 lineageId, 2: bool cascade)
    throws (1: exception.TachyonTException e)

  /**
   * Returns a list of existing lineages.
   * @return a list of existing lineages
   */
  list<LineageInfo> getLineageInfoList()

  /**
   * Reinitializes a file.
   * @param path
   * @param blockSizeBytes
   * @param ttl
   * @return the id of the reinitialized file when the file is lost or not completed, -1 otherwise
   * @throws TachyonTException
   */
  i64 reinitializeFile(1: string path, 2: i64 blockSizeBytes, 3: i64 ttl)
    throws (1: exception.TachyonTException e)

  /**
   * Reports file as lost.
   * @param path
   * @throws TachyonTException
   */
  void reportLostFile(1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Periodic lineage worker heartbeat.
   * @param workerId
   * @param persistedFiles
   * @return the command for checkpointing the blocks of a file
   * @throws TachyonTException
   */
  LineageCommand workerLineageHeartbeat(1: i64 workerId, 2: list<i64> persistedFiles)
    throws (1: exception.TachyonTException e)
}
