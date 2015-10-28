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

service LineageMasterService {

  // Tachyon Client API

  /*
   * Marks a file as completed and initiates asynchronous persistence (if applicable).
   */
  void asyncCompleteFile(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /*
   * Creates a lineage.
   */
  i64 createLineage(1: list<string> inputFiles, 2: list<string> outputFiles, 3: CommandLineJobInfo job)
    throws (1: exception.TachyonTException e)

  /*
   * Deletes a lineage.
   */
  bool deleteLineage(1: i64 lineageId, 2: bool cascade)
    throws (1: exception.TachyonTException e)

  /*
   * Returns a list of existing lineages.
   */
  list<LineageInfo> getLineageInfoList()

  /*
   * Reinitializes a file.
   */
  i64 reinitializeFile(1: string path, 2: i64 blockSizeBytes, 3: i64 ttl)
    throws (1: exception.TachyonTException e)

  /*
   * Reports file as lost.
   */
  void reportLostFile(1: string path)
    throws (1: exception.TachyonTException e)

  // Tachyon Worker API

  /*
   * Periodic lineage worker heartbeat.
   */
  LineageCommand workerLineageHeartbeat(1: i64 workerId, 2: list<i64> persistedFiles)
}
