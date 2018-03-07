namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct GetMasterInfoTOptions {
  1: set<MasterInfoField> filter
}

enum MasterInfoField {
  WEB_PORT
}

struct GetMasterInfoTResponse {
  1: MasterInfo masterInfo
}

struct MasterInfo {
  1: i32 webPort
}

struct ClusterInfo {
 1: string masterAddress
 2: string startTime
 3: string upTime
 4: string version
 5: bool safeMode
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
