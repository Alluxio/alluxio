namespace java tachyon.thrift

/**
* Contains the information of a block in Tachyon. It maintains the worker nodes where the replicas
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

/**
* Contains the information of a block in a file. In addition to the BlockInfo, it includes the
* offset in the file, and the under file system locations of the block replicas.
*/
struct FileBlockInfo {
  1: BlockInfo blockInfo
  2: i64 offset
  3: list<WorkerNetAddress> ufsLocations
}

enum CommandType {
  Unknown = 0,
  Nothing = 1,
  Register = 2, // Ask the worker to re-register.
  Free = 3,     // Ask the worker to free files.
  Delete = 4,   // Ask the worker to delete files.
  Persist = 5,  // Ask the worker to persist a file for lineage
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
}

/**
* Information about the RPC.
*/
struct RpcOptions {
  // key used to identify retried RPCs
  1: optional string key
}

service TachyonService {

  /**
   * Returns the version of the master service.
   * NOTE: The version should be updated every time a backwards incompatible API change occurs.
   */
  i64 getServiceVersion()
}
