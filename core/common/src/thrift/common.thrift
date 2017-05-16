namespace java alluxio.thrift

include "exception.thrift"

/**
 * Contains the information of a block in Alluxio. It maintains the worker nodes where the replicas
 * of the blocks are stored.
 */
struct BlockInfo {
  1: i64 blockId
  2: i64 length
  3: list<BlockLocation> locations
}

/**
 * Information about blocks.
 */
struct BlockLocation {
  1: i64 workerId
  2: WorkerNetAddress workerAddress
  3: string tierAlias
}

enum CommandType {
  Unknown = 0,
  Nothing = 1,
  Register = 2, // Ask the worker to re-register.
  Free = 3,     // Ask the worker to free files.
  Delete = 4,   // Ask the worker to delete files.
  Persist = 5,  // Ask the worker to persist a file for lineage
}

enum TTtlAction {
  Delete = 0, // Delete the file after TTL expires.
  Free = 1,   // Free the file after TTL expires.
}

struct Command {
  1: CommandType commandType
  2: list<i64> data
}

/**
 * Address information about workers.
 */
struct WorkerNetAddress {
  1: string host
  2: i32 rpcPort
  3: i32 dataPort
  4: i32 webPort
  5: string domainSocketPath
}

struct GetServiceVersionTResponse {
  1: i64 version,
}

struct GetServiceVersionTOptions {}

service AlluxioService {

  /**
   * Returns the version of the master service.
   * NOTE: The version should be updated every time a backwards incompatible API change occurs.
   */
  GetServiceVersionTResponse getServiceVersion(
    /** the method options */ 1: GetServiceVersionTOptions options,
  ) throws (1: exception.AlluxioTException e)
}
