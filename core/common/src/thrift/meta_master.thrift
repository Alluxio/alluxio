namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct ConfigProperty {
  1: string name
  2: string source
  3: string value
}

struct GetConfigurationTOptions{}
struct GetConfigurationTResponse{
  1: list<ConfigProperty> configList
}

enum MasterInfoField {
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
 1: string masterAddress
 2: i32 rpcPort
 3: bool safeMode
 4: i64 startTimeMs
 5: i64 upTimeMs
 6: string version
 7: i32 webPort
 8: list<string> zookeeperAddresses // Null means zookeeper is not enabled
}

struct ExportJournalTOptions {
 // The directory to export to within the root UFS.
 1: string targetDirectory
 // Whether to write to the local filesystem instead of the root UFS.
 2: bool localFileSystem
}
struct ExportJournalTResponse {
 1: string backupUri
 // If we export to local filesystem, this field will be populated with the hostname of the host we
 // wrote the journal on.
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

// This type is used as a union, only one of doubleValue or longValue should be set
struct MetricValue {
  1: optional double doubleValue;
  2: optional i64 longValue;
}

/**
  * This interface contains meta master service endpoints for Alluxio clients.
  */
service MetaMasterClientService extends common.AlluxioService {
  /**
   * Exports the journal to the specified URI
   */
  ExportJournalTResponse exportJournal(
    /** the method options */ 2: ExportJournalTOptions options,
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
}
