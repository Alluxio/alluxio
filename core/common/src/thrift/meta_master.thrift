namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct GetConfigurationTOptions{}
struct GetConfigurationTResponse{
  1: list<common.ConfigProperty> configList
}

enum MasterInfoField {
  LIVE_MASTER_NUM
  LOST_MASTER_NUM
  MASTER_ADDRESS
  RPC_PORT
  SAFE_MODE
  START_TIME_MS
  UP_TIME_MS
  VERSION
  WEB_PORT
  ZOOKEEPER_ADDRESSES
}

struct MasterInfo {
  1: i32 liveMasterNum
  2: i32 lostMasterNum
  3: string masterAddress
  4: i32 rpcPort
  5: bool safeMode
  6: i64 startTimeMs
  7: i64 upTimeMs
  8: string version
  9: i32 webPort
  10: list<string> zookeeperAddresses // Null means zookeeper is not enabled
}

enum MetaCommand {
  Unknown = 0,
  Nothing = 1,
  Register = 2, // Ask the standby master to re-register.
}

struct GetMasterIdTOptions {}
struct GetMasterIdTResponse {
  1: i64 masterId
}

struct GetMasterInfoTOptions {
  1: set<MasterInfoField> filter
}

struct GetMasterInfoTResponse {
  1: MasterInfo masterInfo
}

struct GetMetricsTOptions {}
struct GetMetricsTResponse {
  1: map<string, MetricValue> metricsMap
}

struct MasterHeartbeatTOptions {}
struct MasterHeartbeatTResponse {
  1: MetaCommand command
}

// This type is used as a union, only one of doubleValue or longValue should be set
struct MetricValue {
  1: optional double doubleValue;
  2: optional i64 longValue;
}

struct RegisterMasterTOptions {
  1: list<common.ConfigProperty> configList
}
struct RegisterMasterTResponse {}

/**
  * This interface contains meta master service endpoints for Alluxio clients.
  */
service MetaMasterClientService extends common.AlluxioService {
  /**
   * Returns a list of Alluxio runtime configuration information.
   */
  GetConfigurationTResponse getConfiguration(
    /** the method options */ 1: GetConfigurationTOptions options,
    ) throws (1: exception.AlluxioTException e)

  /**
   * Returns information about the master.
   */
  GetMasterInfoTResponse getMasterInfo(
    /** the method options */ 1: GetMasterInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns a map of metrics property names and their values from Alluxio metrics system.
   */
  GetMetricsTResponse getMetrics(
    /** the method options */ 1: GetMetricsTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}

/**
  * This interface contains meta master service endpoints for Alluxio standby masters.
  */
service MetaMasterMasterService extends common.AlluxioService {
  /**
   * Returns a master id for the given master address.
   */
  GetMasterIdTResponse getMasterId(
    /** the master address */ 1: common.MasterAddress masterAddress,
    /** the method options */ 2: GetMasterIdTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Registers a master.
   */
  RegisterMasterTResponse registerMaster(
    /** the id of the master */  1: i64 masterId,
    /** the method options */ 2: RegisterMasterTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Periodic standby master heartbeat to indicate the master is lost or not.
   */
  MasterHeartbeatTResponse masterHeartbeat(
    /** the id of the master */ 1: i64 masterId,
    /** the method options */ 2: MasterHeartbeatTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
