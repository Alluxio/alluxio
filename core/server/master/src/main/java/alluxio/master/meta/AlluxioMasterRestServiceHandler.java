/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.meta;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.MasterStorageTierAssoc;
import alluxio.PropertyKey;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.master.MasterProcess;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.StartupConsistencyCheck;
import alluxio.metrics.MetricsSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.LogUtils;
import alluxio.web.MasterWebServer;
import alluxio.wire.AlluxioMasterInfo;
import alluxio.wire.Capacity;
import alluxio.wire.MountPointInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.qmino.miredot.annotations.ReturnType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for requesting general master information.
 */
@NotThreadSafe
@Path(AlluxioMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioMasterRestServiceHandler {

  public static final String SERVICE_PREFIX = "master";

  // endpoints
  public static final String GET_INFO = "info";

  // queries
  public static final String QUERY_RAW_CONFIGURATION = "raw_configuration";

  // log
  public static final String LOG_LEVEL = "logLevel";
  public static final String LOG_ARGUMENT_NAME = "logName";
  public static final String LOG_ARGUMENT_LEVEL = "level";

  // the following endpoints are deprecated
  public static final String GET_RPC_ADDRESS = "rpc_address";
  public static final String GET_CONFIGURATION = "configuration";
  public static final String GET_CAPACITY_BYTES = "capacity_bytes";
  public static final String GET_USED_BYTES = "used_bytes";
  public static final String GET_FREE_BYTES = "free_bytes";
  public static final String GET_CAPACITY_BYTES_ON_TIERS = "capacity_bytes_on_tiers";
  public static final String GET_USED_BYTES_ON_TIERS = "used_bytes_on_tiers";
  public static final String GET_UFS_CAPACITY_BYTES = "ufs_capacity_bytes";
  public static final String GET_UFS_USED_BYTES = "ufs_used_bytes";
  public static final String GET_UFS_FREE_BYTES = "ufs_free_bytes";
  public static final String GET_METRICS = "metrics";
  public static final String GET_START_TIME_MS = "start_time_ms";
  public static final String GET_UPTIME_MS = "uptime_ms";
  public static final String GET_VERSION = "version";
  public static final String GET_WORKER_COUNT = "worker_count";
  public static final String GET_WORKER_INFO_LIST = "worker_info_list";

  private final MasterProcess mMasterProcess;
  private final BlockMaster mBlockMaster;
  private final FileSystemMaster mFileSystemMaster;
  private final String mUfsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
  private final UnderFileSystem mUfs;

  /**
   * Constructs a new {@link AlluxioMasterRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public AlluxioMasterRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mMasterProcess = (MasterProcess) context
        .getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY);
    mBlockMaster = mMasterProcess.getMaster(BlockMaster.class);
    mFileSystemMaster = mMasterProcess.getMaster(FileSystemMaster.class);
    mUfs = UnderFileSystem.Factory.createForRoot();
  }

  /**
   * @summary get the Alluxio master information
   * @param rawConfiguration if it's true, raw configuration values are returned,
   *    otherwise, they are looked up; if it's not provided in URL queries, then
   *    it is null, which means false.
   * @return the response object
   */
  @GET
  @Path(GET_INFO)
  @ReturnType("alluxio.wire.AlluxioMasterInfo")
  public Response getInfo(@QueryParam(QUERY_RAW_CONFIGURATION) final Boolean rawConfiguration) {
    // TODO(jiri): Add a mechanism for retrieving only a subset of the fields.
    return RestUtils.call(new RestUtils.RestCallable<AlluxioMasterInfo>() {
      @Override
      public AlluxioMasterInfo call() throws Exception {
        boolean rawConfig = false;
        if (rawConfiguration != null) {
          rawConfig = rawConfiguration;
        }
        AlluxioMasterInfo result =
            new AlluxioMasterInfo()
                .setCapacity(getCapacityInternal())
                .setConfiguration(getConfigurationInternal(rawConfig))
                .setLostWorkers(mBlockMaster.getLostWorkersInfoList())
                .setMetrics(getMetricsInternal())
                .setMountPoints(getMountPointsInternal())
                .setRpcAddress(mMasterProcess.getRpcAddress().toString())
                .setStartTimeMs(mMasterProcess.getStartTimeMs())
                .setStartupConsistencyCheck(getStartupConsistencyCheckInternal())
                .setTierCapacity(getTierCapacityInternal())
                .setUfsCapacity(getUfsCapacityInternal())
                .setUptimeMs(mMasterProcess.getUptimeMs())
                .setVersion(RuntimeConstants.VERSION)
                .setWorkers(mBlockMaster.getWorkerInfoList());
        return result;
      }
    });
  }

  /**
   * @summary get the configuration map, the keys are ordered alphabetically.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_CONFIGURATION)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.String>")
  @Deprecated
  public Response getConfiguration() {
    return RestUtils.call(() -> getConfigurationInternal(true));
  }

  /**
   * @summary get the master metrics, the keys are ordered alphabetically.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_METRICS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  @Deprecated
  public Response getMetrics() {
    return RestUtils.call(() -> getMetricsInternal());
  }

  /**
   * @summary get the master rpc address
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_RPC_ADDRESS)
  @ReturnType("java.lang.String")
  @Deprecated
  public Response getRpcAddress() {
    return RestUtils.call(() -> mMasterProcess.getRpcAddress().toString());
  }

  /**
   * @summary get the start time of the master
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_START_TIME_MS)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getStartTimeMs() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mMasterProcess.getStartTimeMs();
      }
    });
  }

  /**
   * @summary get the uptime of the master
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_UPTIME_MS)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getUptimeMs() {
    return RestUtils.call(() -> mMasterProcess.getUptimeMs());
  }

  /**
   * @summary get the version of the master
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_VERSION)
  @ReturnType("java.lang.String")
  @Deprecated
  public Response getVersion() {
    return RestUtils.call(() -> RuntimeConstants.VERSION);
  }

  /**
   * @summary get the total capacity of all workers in bytes
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getCapacityBytes() {
    return RestUtils.call(() -> mBlockMaster.getCapacityBytes());
  }

  /**
   * @summary get the used capacity
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_USED_BYTES)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getUsedBytes() {
    return RestUtils.call(() -> mBlockMaster.getUsedBytes());
  }

  /**
   * @summary get the free capacity
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_FREE_BYTES)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getFreeBytes() {
    return RestUtils.call(() -> mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes());
  }

  /**
   * @summary get the total ufs capacity in bytes, a negative value means the capacity is unknown.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_UFS_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getUfsCapacityBytes() {
    return RestUtils.call(() -> mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_USED));
  }

  /**
   * @summary get the used disk capacity, a negative value means the capacity is unknown.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_UFS_USED_BYTES)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getUfsUsedBytes() {
    return RestUtils.call(() -> mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_USED));
  }

  /**
   * @summary get the free ufs capacity in bytes, a negative value means the capacity is unknown.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_UFS_FREE_BYTES)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getUfsFreeBytes() {
    return RestUtils.call(() -> mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_FREE));
  }

  private Comparator<String> getTierAliasComparator() {
    return new Comparator<String>() {
      private MasterStorageTierAssoc mTierAssoc = new MasterStorageTierAssoc();

      @Override
      public int compare(String tier1, String tier2) {
        int ordinal1 = mTierAssoc.getOrdinal(tier1);
        int ordinal2 = mTierAssoc.getOrdinal(tier2);
        if (ordinal1 < ordinal2) {
          return -1;
        }
        if (ordinal1 == ordinal2) {
          return 0;
        }
        return 1;
      }
    };
  }

  /**
   * @summary get the mapping from tier alias to total capacity of the tier in bytes, keys are in
   *    the order from tier alias with smaller ordinal to those with larger ones.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_CAPACITY_BYTES_ON_TIERS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  @Deprecated
  public Response getCapacityBytesOnTiers() {
    return RestUtils.call((RestUtils.RestCallable<Map<String, Long>>) () -> {
      SortedMap<String, Long> capacityBytesOnTiers = new TreeMap<>(getTierAliasComparator());
      for (Map.Entry<String, Long> tierBytes : mBlockMaster.getTotalBytesOnTiers().entrySet()) {
        capacityBytesOnTiers.put(tierBytes.getKey(), tierBytes.getValue());
      }
      return capacityBytesOnTiers;
    });
  }

  /**
   * @summary get the mapping from tier alias to the used bytes of the tier, keys are in the order
   *    from tier alias with smaller ordinal to those with larger ones.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_USED_BYTES_ON_TIERS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  @Deprecated
  public Response getUsedBytesOnTiers() {
    return RestUtils.call((RestUtils.RestCallable<Map<String, Long>>) () -> {
      SortedMap<String, Long> usedBytesOnTiers = new TreeMap<>(getTierAliasComparator());
      for (Map.Entry<String, Long> tierBytes : mBlockMaster.getUsedBytesOnTiers().entrySet()) {
        usedBytesOnTiers.put(tierBytes.getKey(), tierBytes.getValue());
      }
      return usedBytesOnTiers;
    });
  }

  /**
   * @summary get the count of workers
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_WORKER_COUNT)
  @ReturnType("java.lang.Integer")
  @Deprecated
  public Response getWorkerCount() {
    return RestUtils.call(()->mBlockMaster.getWorkerCount());
  }

  /**
   * @summary get the list of worker descriptors
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_WORKER_INFO_LIST)
  @ReturnType("java.util.List<alluxio.wire.WorkerInfo>")
  @Deprecated
  public Response getWorkerInfoList() {
    return RestUtils.call(()-> mBlockMaster.getWorkerInfoList());
  }

  private Capacity getCapacityInternal() {
    return new Capacity().setTotal(mBlockMaster.getCapacityBytes())
        .setUsed(mBlockMaster.getUsedBytes());
  }

  private Map<String, String> getConfigurationInternal(boolean raw) {
    Set<Map.Entry<String, String>> properties = Configuration.toMap().entrySet();
    SortedMap<String, String> configuration = new TreeMap<>();
    for (Map.Entry<String, String> entry : properties) {
      String key = entry.getKey();
      if (PropertyKey.isValid(key)) {
        if (raw) {
          configuration.put(key, entry.getValue());
        } else {
          configuration.put(key, Configuration.get(PropertyKey.fromString(key)));
        }
      }
    }
    return configuration;
  }

  private Map<String, Long> getMetricsInternal() {
    MetricRegistry metricRegistry = MetricsSystem.METRIC_REGISTRY;

    // Get all counters.
    Map<String, Counter> counters = metricRegistry.getCounters();
    // Only the gauge for pinned files is retrieved here, other gauges are statistics of
    // free/used
    // spaces, those statistics can be gotten via other REST apis.
    String filesPinnedProperty =
        MetricsSystem.getMasterMetricName(DefaultFileSystemMaster.Metrics.FILES_PINNED);
    @SuppressWarnings("unchecked") Gauge<Integer> filesPinned =
        (Gauge<Integer>) MetricsSystem.METRIC_REGISTRY.getGauges().get(filesPinnedProperty);

    // Get values of the counters and gauges and put them into a metrics map.
    SortedMap<String, Long> metrics = new TreeMap<>();
    for (Map.Entry<String, Counter> counter : counters.entrySet()) {
      metrics.put(counter.getKey(), counter.getValue().getCount());
    }
    metrics.put(filesPinnedProperty, filesPinned.getValue().longValue());
    return metrics;
  }

  private Map<String, MountPointInfo> getMountPointsInternal() {
    return mFileSystemMaster.getMountTable();
  }

  private alluxio.wire.StartupConsistencyCheck getStartupConsistencyCheckInternal() {
    StartupConsistencyCheck check = mFileSystemMaster.getStartupConsistencyCheck();
    alluxio.wire.StartupConsistencyCheck ret = new alluxio.wire.StartupConsistencyCheck();
    List<AlluxioURI> inconsistentUris = check.getInconsistentUris();
    List<String> uris = new ArrayList<>(inconsistentUris.size());
    for (AlluxioURI uri : inconsistentUris) {
      uris.add(uri.toString());
    }
    ret.setInconsistentUris(uris);
    ret.setStatus(check.getStatus().toString().toLowerCase());
    return ret;
  }

  private Map<String, Capacity> getTierCapacityInternal() {
    SortedMap<String, Capacity> tierCapacity = new TreeMap<>();
    Map<String, Long> totalTierCapacity = mBlockMaster.getTotalBytesOnTiers();
    Map<String, Long> usedTierCapacity = mBlockMaster.getUsedBytesOnTiers();
    for (String tierAlias : mBlockMaster.getGlobalStorageTierAssoc().getOrderedStorageAliases()) {
      long total = totalTierCapacity.containsKey(tierAlias) ? totalTierCapacity.get(tierAlias) : 0;
      long used = usedTierCapacity.containsKey(tierAlias) ? usedTierCapacity.get(tierAlias) : 0;
      tierCapacity.put(tierAlias, new Capacity().setTotal(total).setUsed(used));
    }
    return tierCapacity;
  }

  private Capacity getUfsCapacityInternal() throws IOException {
    return new Capacity().setTotal(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL))
        .setUsed(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_USED));
  }

  /**
   * @summary set the Alluxio log information
   * @param logName the log's name
   * @param level the log level
   * @return the response object
   */
  @POST
  @Path(LOG_LEVEL)
  @ReturnType("alluxio.wire.LogInfo")
  public Response logLevel(@QueryParam(LOG_ARGUMENT_NAME) final String logName, @QueryParam
      (LOG_ARGUMENT_LEVEL) final String level) {
    return RestUtils.call(() -> LogUtils.setLogLevel(logName, level));
  }
}
