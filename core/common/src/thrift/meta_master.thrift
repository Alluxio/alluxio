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
