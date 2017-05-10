namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct MasterInfo {
  1: i32 webPort
}

enum MasterInfoField {
  WEB_PORT
}

/**
  * This interface contains meta master service endpoints for Alluxio clients.
  */
service MetaMasterClientService extends common.AlluxioService {

  /**
   * Returns information about the master.
   */
  MasterInfo getInfo(
    /** optional filter for what fields to return, defaults to all */
    1: set<MasterInfoField> fields,
    )
    throws (1: exception.AlluxioTException e)
}
