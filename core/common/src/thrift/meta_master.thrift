namespace java alluxio.thrift

include "common.thrift"

struct MasterInfo {
  1: i64 rpcPort
  2: i64 webPort
}

/**
  * This interface contains meta master service endpoints for Alluxio clients.
  */
service MetaMasterClientService extends common.AlluxioService {

  /**
   * Returns information about the master.
   */
  MasterInfo getMasterInfo(
    /** optional filter for what fields to return, defaults to all */
    1: list<string> fieldNames,
    )
}