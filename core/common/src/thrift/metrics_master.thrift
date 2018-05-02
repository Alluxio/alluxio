namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct MetricsHeartbeatTOptions {
  1: list<common.Metric> metrics
}

struct MetricsHeartbeatTResponse {}

/**
 * This interface contains metrics master service endpoints for Alluxio clients.
 */
service MetricsMasterClientService extends common.AlluxioService {

  /**
   * Periodic metrics master client heartbeat.
   */
  MetricsHeartbeatTResponse metricsHeartbeat(
    /** the id of the client */ 1: string clientId,
    /** the client hostname */ 2: string hostname,    
    /** the method options */ 3: MetricsHeartbeatTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}