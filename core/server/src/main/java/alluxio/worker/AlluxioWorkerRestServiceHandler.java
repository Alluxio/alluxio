/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker;

import alluxio.Version;
import alluxio.collections.Pair;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockStoreMeta;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.qmino.miredot.annotations.ReturnType;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for requesting general worker information.
 */
@NotThreadSafe
@Path(AlluxioWorkerRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioWorkerRestServiceHandler {
  public static final String SERVICE_PREFIX = "worker";
  public static final String GET_RPC_ADDRESS = "rpc_address";
  public static final String GET_CAPACITY_BYTES = "capacity_bytes";
  public static final String GET_USED_BYTES = "used_bytes";
  public static final String GET_CAPACITY_BYTES_ON_TIERS = "capacity_bytes_on_tiers";
  public static final String GET_USED_BYTES_ON_TIERS = "used_bytes_on_tiers";
  public static final String GET_DIRECTORY_PATHS_ON_TIERS = "directory_paths_on_tiers";
  public static final String GET_START_TIME_MS = "start_time_ms";
  public static final String GET_UPTIME_MS = "uptime_ms";
  public static final String GET_VERSION = "version";
  public static final String GET_METRICS = "metrics";

  private final AlluxioWorker mWorker = AlluxioWorker.get();
  private final BlockStoreMeta mStoreMeta = mWorker.getBlockWorker().getStoreMeta();

  /**
   * @summary get the address of the worker
   * @return the response object
   */
  @GET
  @Path(GET_RPC_ADDRESS)
  @ReturnType("java.lang.String")
  public Response getRpcAddress() {
    return Response.ok(mWorker.getWorkerAddress().toString()).build();
  }

  /**
   * @summary get the total capacity of the worker in bytes
   * @return the response object
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  public Response getCapacityBytes() {
    return Response.ok(mStoreMeta.getCapacityBytes()).build();
  }

  /**
   * @summary get the used bytes of the worker
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUsedBytes() {
    return Response.ok(mStoreMeta.getUsedBytes()).build();
  }

  /**
   * @summary get the mapping from tier alias to the total capacity of the tier in bytes
   * @return the response object
   */
  @GET
  @Path(GET_CAPACITY_BYTES_ON_TIERS)
  @ReturnType("java.util.Map<String, Long>")
  public Response getCapacityBytesOnTiers() {
    return Response.ok(mStoreMeta.getCapacityBytesOnTiers()).build();
  }

  /**
   * @summary get the mapping from tier alias to the used bytes of the tier
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES_ON_TIERS)
  @ReturnType("java.util.Map<String, Long>")
  public Response getUsedBytesOnTiers() {
    return Response.ok(mStoreMeta.getUsedBytesOnTiers()).build();
  }

  /**
   * @summary get the mapping from tier alias to the paths of the directories in the tier
   * @return the response object
   */
  @GET
  @Path(GET_DIRECTORY_PATHS_ON_TIERS)
  @ReturnType("java.util.Map<String, String>")
  public Response getDirectoryPathsOnTiers() {
    Map<String, String> tierToDirPaths = new HashMap<>(mStoreMeta.getCapacityBytesOnDirs().size());
    for (Pair<String, String> tierToDirPath : mStoreMeta.getCapacityBytesOnDirs().keySet()) {
      tierToDirPaths.put(tierToDirPath.getFirst(), tierToDirPath.getSecond());
    }
    return Response.ok(tierToDirPaths).build();
  }

  /**
   * @summary get the version of the worker
   * @return the response object
   */
  @GET
  @Path(GET_VERSION)
  @ReturnType("java.lang.String")
  public Response getVersion() {
    return Response.ok(Version.VERSION).build();
  }

  /**
   * @summary get the start time of the worker in milliseconds
   * @return the response object
   */
  @GET
  @Path(GET_START_TIME_MS)
  @ReturnType("java.lang.Long")
  public Response getStartTimeMs() {
    return Response.ok(mWorker.getStartTimeMs()).build();
  }

  /**
   * @summary get the uptime of the worker in milliseconds
   * @return the response object
   */
  @GET
  @Path(GET_UPTIME_MS)
  @ReturnType("java.lang.Long")
  public Response getUptimeMs() {
    return Response.ok(System.currentTimeMillis() - mWorker.getStartTimeMs()).build();
  }

  /**
   * @summary get the worker metrics
   * @return the response object
   */
  @GET
  @Path(GET_METRICS)
  @ReturnType("java.util.Map<String, Long>")
  public Response getMetrics() {
    MetricRegistry metricRegistry = mWorker.getWorkerMetricsSystem().getMetricRegistry();

    // Get all counters.
    Map<String, Counter> counters = metricRegistry.getCounters();

    // Only the gauge for cached blocks is retrieved here, other gauges are statistics of free/used
    // spaces, those statistics can be gotten via other REST apis.
    String blocksCachedProperty = CommonUtils.argsToString(".",
        WorkerContext.getWorkerSource().getName(), WorkerSource.BLOCKS_CACHED);
    Gauge blocksCached = metricRegistry.getGauges().get(blocksCachedProperty);

    // Get values of the counters and gauges and put them into a metrics map.
    Map<String, Long> metrics = new HashMap<>(counters.size() + 1);
    for (Map.Entry<String, Counter> counter : counters.entrySet()) {
      metrics.put(counter.getKey(), counter.getValue().getCount());
    }
    metrics.put(blocksCachedProperty, ((Integer) blocksCached.getValue()).longValue());

    return Response.ok(metrics).build();
  }
}
