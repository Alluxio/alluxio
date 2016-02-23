namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct CommandLineJobInfo {
  1: string command
  2: JobConfInfo conf
}

struct DependencyInfo{} // deprecated

struct JobConfInfo {
  1: string outputFile
}

struct LineageInfo {
  1: i64 id
  2: list<string> inputFiles
  3: list<string> outputFiles
  4: CommandLineJobInfo job
  5: i64 creationTimeMs
  6: list<i64> parents
  7: list<i64> children
}

/**
 * This interface contains lineage master service endpoints for Alluxio clients.
 */
service LineageMasterClientService extends common.AlluxioService {

  /**
   * Creates a lineage and returns the lineage id.
   */
  i64 createLineage( /** the list of input files */ 1: list<string> inputFiles,
      /** the list of output files */ 2: list<string> outputFiles,
      /** the command line job info */ 3: CommandLineJobInfo job)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Deletes a lineage and returns whether the deletion succeeded.
   */
  bool deleteLineage( /** the lineage id */ 1: i64 lineageId,
      /** whether to delete the lineage in cascade */ 2: bool cascade)
    throws (1: exception.AlluxioTException e)

  /**
   * Returns a list of existing lineages.
   */
  list<LineageInfo> getLineageInfoList()

  /**
   * Reinitializes a file. Returns the id of the reinitialized file when the
   * file is lost or not completed, -1 otherwise.
   */
  i64 reinitializeFile( /** the path of the file */ 1: string path,
      /** block size in bytes */ 2: i64 blockSizeBytes,
      /** time to live */ 3: i64 ttl)
    throws (1: exception.AlluxioTException e)

  /**
   * Reports file as lost.
   */
  void reportLostFile( /** the path of the file */ 1: string path)
    throws (1: exception.AlluxioTException e)
}
