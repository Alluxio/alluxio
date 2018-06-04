namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct GetConfigurationTOptions{}
struct GetConfigurationTResponse{
  1: list<common.ConfigProperty> configList
}

enum MasterInfoField {
  LEADER_MASTER_ADDRESS
  MASTER_ADDRESSES
  RPC_PORT
  SAFE_MODE
  START_TIME_MS
  UP_TIME_MS
  VERSION
  WEB_PORT
  WORKER_ADDRESSES
  ZOOKEEPER_ADDRESSES
}

struct MasterInfo {
  1: string leaderMasterAddress
  2: list<common.MasterNetAddress> masterAddresses
  3: i32 rpcPort
  4: bool safeMode
  5: i64 startTimeMs
  6: i64 upTimeMs
  7: string version
  8: i32 webPort
  9: list<common.MasterNetAddress> workerAddresses
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

struct BackupTOptions {
 // The directory to write a backup to within the root UFS.
 1: string targetDirectory
 // Whether to write to the local filesystem instead of the root UFS.
 2: bool localFileSystem
}
struct BackupTResponse {
 // The URI of the created backup file.
 1: string backupUri
 // The hostname of the master which wrote the backup. This is useful
 // when the backup was written to local disk of that host.
 2: string hostname
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

enum ConfigStatus {
  PASSED
  WARN
  FAILED
}

enum Scope {
  MASTER
  WORKER
  CLIENT
  SERVER
  ALL
  NONE
}

struct InconsistentProperty {
  1: string name
  2: map<string, list<string>> values
}

struct ConfigCheckReport {
  1: map<Scope, list<InconsistentProperty>> errors
  2: map<Scope, list<InconsistentProperty>> warns
  3: ConfigStatus status
}

struct GetConfigReportTOptions {}

struct GetConfigReportTResponse {
  1: ConfigCheckReport report
}

/**
  * This interface contains meta master service endpoints for Alluxio clients.
  */
service MetaMasterClientService extends common.AlluxioService {
  /**
   * Backs up the Alluxio master to the specified URI
   */
  BackupTResponse backup(
    /** the method options */ 2: BackupTOptions options,
    ) throws (1: exception.AlluxioTException e)

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

  /**
   * Returns server-side configuration report.
   */
  GetConfigReportTResponse getConfigReport(
    /** the method options */ 1: GetConfigReportTOptions options,
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
    /** the master address */ 1: common.MasterNetAddress masterAddress,
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
   * Heartbeats to indicate the master is lost or not.
   */
  MasterHeartbeatTResponse masterHeartbeat(
    /** the id of the master */ 1: i64 masterId,
    /** the method options */ 2: MasterHeartbeatTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
