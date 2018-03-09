namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct ClusterInfo {
 1: string masterAddress
 2: i32 webPort
 3: i32 rpcPort
 4: string startTime
 5: string upTime
 6: string version
 7: bool safeMode
}

struct MasterInfo {
  1: i32 webPort
}

enum MasterInfoField {
  WEB_PORT
}

struct GetMasterInfoTOptions {
  1: set<MasterInfoField> filter
}

struct GetMasterInfoTResponse {
  1: MasterInfo masterInfo
}

struct GetClusterInfoTOptions {}

struct GetClusterInfoTResponse {
 1: ClusterInfo clusterInfo
}

/**
  * This interface contains meta master service endpoints for Alluxio clients.
  */
service MetaMasterClientService extends common.AlluxioService {

  /**
   * Returns information about the Alluxio running cluster.
   */
  GetClusterInfoTResponse getClusterInfo(
    /** the method options */ 1: GetClusterInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns information about the master.
   */
  GetMasterInfoTResponse getMasterInfo(
    /** the method options */ 1: GetMasterInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
