namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct MasterInfo {
 1: string masterAddress
 2: i32 webPort
 3: i32 rpcPort
 4: i64 startTimeMs
 5: i64 upTimeMs
 6: string version
 7: bool safeMode
}

struct GetMasterInfoTOptions {}

struct GetMasterInfoTResponse {
  1: MasterInfo masterInfo
}

/**
  * This interface contains meta master service endpoints for Alluxio clients.
  */
service MetaMasterClientService extends common.AlluxioService {
  /**
   * Returns information about the master.
   */
  GetMasterInfoTResponse getMasterInfo(
    /** the method options */ 1: GetMasterInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
