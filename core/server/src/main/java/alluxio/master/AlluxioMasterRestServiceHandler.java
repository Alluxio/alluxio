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

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Version;
import alluxio.master.block.BlockMaster;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.qmino.miredot.annotations.ReturnType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for requesting general master information.
 */
@NotThreadSafe
@Path(AlluxioMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
// TODO(cc): Investigate auto-generation of REST API documentation.
public final class AlluxioMasterRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SERVICE_PREFIX = "master";
  public static final String GET_RPC_ADDRESS = "rpc_address";
  public static final String GET_CONFIGURATION = "configuration";
  public static final String GET_CAPACITY_BYTES = "capacity_bytes";
  public static final String GET_USED_BYTES = "used_bytes";
  public static final String GET_FREE_BYTES = "free_bytes";
  public static final String GET_CAPACITY_BYTES_ON_TIERS = "capacity_bytes_on_tiers";
  public static final String GET_USED_BYTES_ON_TIERS = "used_bytes_on_tiers";
  public static final String GET_DISK_CAPACITY_BYTES = "disk_capacity_bytes";
  public static final String GET_DISK_FREE_BYTES = "disk_free_bytes";
  public static final String GET_DISK_USED_BYTES = "disk_used_bytes";
  public static final String GET_METRICS = "metrics";
  public static final String GET_START_TIME_MS = "start_time_ms";
  public static final String GET_UPTIME_MS = "uptime_ms";
  public static final String GET_VERSION = "version";
  public static final String GET_WORKER_COUNT = "worker_count";
  public static final String GET_WORKER_INFO_LIST = "worker_info_list";

  private final AlluxioMaster mMaster = AlluxioMaster.get();
  private final BlockMaster mBlockMaster = mMaster.getBlockMaster();
  private final Configuration mMasterConf = MasterContext.getConf();
  private final String mUfsRoot = mMasterConf.get(Constants.UNDERFS_ADDRESS);
  private final UnderFileSystem mUfs = UnderFileSystem.get(mUfsRoot, mMasterConf);

  /**
   * @summary get the configuration map
   * @return the response object
   */
  @GET
  @Path(GET_CONFIGURATION)
  @ReturnType("java.util.Map<String, String>")
  public Response getConfiguration() {
    Set<Map.Entry<Object, Object>> properties = mMasterConf.getInternalProperties().entrySet();
    Map<String, String> configuration = new HashMap<>(properties.size());
    for (Map.Entry<Object, Object> entry : properties) {
      String key = entry.getKey().toString();
      configuration.put(key, mMasterConf.get(key));
    }
    return Response.ok(configuration).build();
  }

  /**
   * @summary get the master rpc address
   * @return the response object
   */
  @GET
  @Path(GET_RPC_ADDRESS)
  @ReturnType("java.lang.String")
  public Response getRpcAddress() {
    return Response.ok(mMaster.getMasterAddress().toString()).build();
  }

  /**
   * @summary get the master metrics
   * @return the response object
   */
  @GET
  @Path(GET_METRICS)
  @ReturnType("java.util.Map<String, Long>")
  public Response getMetrics() {
    MetricRegistry metricRegistry = mMaster.getMasterMetricsSystem().getMetricRegistry();

    // Get all counters.
    Map<String, Counter> counters = metricRegistry.getCounters();

    // Only the gauge for pinned files is retrieved here, other gauges are statistics of free/used
    // spaces, those statistics can be gotten via other REST apis.
    String filesPinnedProperty = CommonUtils.argsToString(".",
        MasterContext.getMasterSource().getName(), MasterSource.FILES_PINNED);
    Gauge filesPinned = metricRegistry.getGauges().get(filesPinnedProperty);

    // Get values of the counters and gauges and put them into a metrics map.
    Map<String, Long> metrics = new HashMap<>(counters.size() + 1);
    for (Map.Entry<String, Counter> counter : counters.entrySet()) {
      metrics.put(counter.getKey(), counter.getValue().getCount());
    }
    metrics.put(filesPinnedProperty, ((Integer) filesPinned.getValue()).longValue());

    return Response.ok(metrics).build();
  }

  /**
   * @summary get the uptime of the master
   * @return the response object
   */
  @GET
  @Path(GET_UPTIME_MS)
  @ReturnType("java.lang.Long")
  public Response getUptimeMs() {
    return Response.ok(System.currentTimeMillis() - mMaster.getStarttimeMs()).build();
  }

  /**
   * @summary get the start time of the master
   * @return the response object
   */
  @GET
  @Path(GET_START_TIME_MS)
  @ReturnType("java.lang.Long")
  public Response getStartTimeMs() {
    return Response.ok(mMaster.getStarttimeMs()).build();
  }

  /**
   * @summary get the version of the master
   * @return the response object
   */
  @GET
  @Path(GET_VERSION)
  @ReturnType("java.lang.String")
  public Response getVersion() {
    return Response.ok(Version.VERSION).build();
  }

  /**
   * @summary get the total capacity of all workers in bytes
   * @return the response object
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  public Response getCapacityBytes() {
    return Response.ok(mBlockMaster.getCapacityBytes()).build();
  }

  /**
   * @summary get the total disk capacity in bytes, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_DISK_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  public Response getDiskCapacityBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @summary get the free disk capacity in bytes, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_DISK_FREE_BYTES)
  @ReturnType("java.lang.Long")
  public Response getFreeDiskCapacityBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_FREE)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @summary get the used disk capacity, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_DISK_USED_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUsedDiskCapacityBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_USED)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @summary get the free capacity
   * @return the response object
   */
  @GET
  @Path(GET_FREE_BYTES)
  @ReturnType("java.lang.Long")
  public Response getFreeCapacityBytes() {
    return Response.ok(mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes()).build();
  }

  /**
   * @summary get the mapping from tier alias to total capacity of the tier in bytes
   * @return the response object
   */
  @GET
  @Path(GET_CAPACITY_BYTES_ON_TIERS)
  @ReturnType("java.util.Map<String, Long>")
  public Response getCapacityBytesOnTiers() {
    return Response.ok(mBlockMaster.getTotalBytesOnTiers()).build();
  }

  /**
   * @summary get the used capacity
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUsedBytes() {
    return Response.ok(mBlockMaster.getUsedBytes()).build();
  }

  /**
   * @summary get the mapping from tier alias to the used bytes of the tier
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES_ON_TIERS)
  @ReturnType("java.util.Map<String, Long>")
  public Response getUsedBytesOnTiers() {
    return Response.ok(mBlockMaster.getUsedBytesOnTiers()).build();
  }

  /**
   * @summary get the count of workers
   * @return the response object
   */
  @GET
  @Path(GET_WORKER_COUNT)
  @ReturnType("java.lang.Integer")
  public Response getWorkerCount() {
    return Response.ok(mBlockMaster.getWorkerCount()).build();
  }

  /**
   * @summary get the list of worker descriptors
   * @return the response object
   */
  @GET
  @Path(GET_WORKER_INFO_LIST)
  @ReturnType("java.util.List<alluxio.wire.WorkerInfo>")
  public Response getWorkerInfoList() {
    return Response.ok(mBlockMaster.getWorkerInfoList()).build();
  }
}
