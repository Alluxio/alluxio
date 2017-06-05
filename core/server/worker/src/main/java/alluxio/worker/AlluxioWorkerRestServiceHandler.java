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

package alluxio.worker;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.WorkerStorageTierAssoc;
import alluxio.metrics.MetricsSystem;
import alluxio.util.LogUtils;
import alluxio.web.WorkerWebServer;
import alluxio.wire.AlluxioWorkerInfo;
import alluxio.wire.Capacity;
import alluxio.wire.LogInfo;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.qmino.miredot.annotations.ReturnType;

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
 * This class is a REST handler for requesting general worker information.
 */
@NotThreadSafe
@Path(AlluxioWorkerRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioWorkerRestServiceHandler {
  public static final String SERVICE_PREFIX = "worker";

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
  public static final String GET_CAPACITY_BYTES = "capacity_bytes";
  public static final String GET_CONFIGURATION = "configuration";
  public static final String GET_USED_BYTES = "used_bytes";
  public static final String GET_CAPACITY_BYTES_ON_TIERS = "capacity_bytes_on_tiers";
  public static final String GET_USED_BYTES_ON_TIERS = "used_bytes_on_tiers";
  public static final String GET_DIRECTORY_PATHS_ON_TIERS = "directory_paths_on_tiers";
  public static final String GET_START_TIME_MS = "start_time_ms";
  public static final String GET_UPTIME_MS = "uptime_ms";
  public static final String GET_VERSION = "version";
  public static final String GET_METRICS = "metrics";

  private final WorkerProcess mWorkerProcess;
  private final BlockStoreMeta mStoreMeta;

  /**
   * @param context context for the servlet
   */
  public AlluxioWorkerRestServiceHandler(@Context ServletContext context) {
    mWorkerProcess = (WorkerProcess) context
        .getAttribute(WorkerWebServer.ALLUXIO_WORKER_SERVLET_RESOURCE_KEY);
    mStoreMeta = mWorkerProcess.getWorker(BlockWorker.class).getStoreMeta();
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
  @ReturnType("alluxio.wire.AlluxioWorkerInfo")
  public Response getInfo(@QueryParam(QUERY_RAW_CONFIGURATION) final Boolean rawConfiguration) {
    // TODO(jiri): Add a mechanism for retrieving only a subset of the fields.
    return RestUtils.call(new RestUtils.RestCallable<AlluxioWorkerInfo>() {
      @Override
      public AlluxioWorkerInfo call() throws Exception {
        boolean rawConfig = false;
        if (rawConfiguration != null) {
          rawConfig = rawConfiguration;
        }
        AlluxioWorkerInfo result =
            new AlluxioWorkerInfo()
                .setCapacity(getCapacityInternal())
                .setConfiguration(getConfigurationInternal(rawConfig))
                .setMetrics(getMetricsInternal())
                .setRpcAddress(mWorkerProcess.getRpcAddress().toString())
                .setStartTimeMs(mWorkerProcess.getStartTimeMs())
                .setTierCapacity(getTierCapacityInternal())
                .setTierPaths(getTierPathsInternal())
                .setUptimeMs(mWorkerProcess.getUptimeMs())
                .setVersion(RuntimeConstants.VERSION);
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
    return RestUtils.call(new RestUtils.RestCallable<Map<String, String>>() {
      @Override
      public Map<String, String> call() throws Exception {
        return getConfigurationInternal(true);
      }
    });
  }

  /**
   * @summary get the address of the worker
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_RPC_ADDRESS)
  @ReturnType("java.lang.String")
  @Deprecated
  public Response getRpcAddress() {
    return RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return mWorkerProcess.getRpcAddress().toString();
      }
    });
  }

  /**
   * @summary get the total capacity of the worker in bytes
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getCapacityBytes() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mStoreMeta.getCapacityBytes();
      }
    });
  }

  /**
   * @summary get the used bytes of the worker
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_USED_BYTES)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getUsedBytes() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mStoreMeta.getUsedBytes();
      }
    });
  }

  /**
   * @summary get the mapping from tier alias to the total capacity of the tier in bytes, the keys
   *    are in the order from tier aliases with smaller ordinals to those with larger ones.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_CAPACITY_BYTES_ON_TIERS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  @Deprecated
  public Response getCapacityBytesOnTiers() {
    return RestUtils.call(new RestUtils.RestCallable<Map<String, Long>>() {
      @Override
      public Map<String, Long> call() throws Exception {
        SortedMap<String, Long> capacityBytesOnTiers = new TreeMap<>(getTierAliasComparator());
        for (Map.Entry<String, Long> tierBytes : mStoreMeta.getCapacityBytesOnTiers().entrySet()) {
          capacityBytesOnTiers.put(tierBytes.getKey(), tierBytes.getValue());
        }
        return capacityBytesOnTiers;
      }
    });
  }

  /**
   * @summary get the mapping from tier alias to the used bytes of the tier, the keys are in the
   *    order from tier aliases with smaller ordinals to those with larger ones.
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_USED_BYTES_ON_TIERS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  @Deprecated
  public Response getUsedBytesOnTiers() {
    return RestUtils.call(new RestUtils.RestCallable<Map<String, Long>>() {
      @Override
      public Map<String, Long> call() throws Exception {
        SortedMap<String, Long> usedBytesOnTiers = new TreeMap<>(getTierAliasComparator());
        for (Map.Entry<String, Long> tierBytes : mStoreMeta.getUsedBytesOnTiers().entrySet()) {
          usedBytesOnTiers.put(tierBytes.getKey(), tierBytes.getValue());
        }
        return usedBytesOnTiers;
      }
    });
  }

  /**
   * @summary get the mapping from tier alias to the paths of the directories in the tier
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_DIRECTORY_PATHS_ON_TIERS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.util.List<java.lang.String>>")
  @Deprecated
  public Response getDirectoryPathsOnTiers() {
    return RestUtils.call(new RestUtils.RestCallable<Map<String, List<String>>>() {
      @Override
      public Map<String, List<String>> call() throws Exception {
        return getTierPathsInternal();
      }
    });
  }

  /**
   * @summary get the version of the worker
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_VERSION)
  @ReturnType("java.lang.String")
  @Deprecated
  public Response getVersion() {
    return RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return RuntimeConstants.VERSION;
      }
    });
  }

  /**
   * @summary get the start time of the worker in milliseconds
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
        return mWorkerProcess.getStartTimeMs();
      }
    });
  }

  /**
   * @summary get the uptime of the worker in milliseconds
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_UPTIME_MS)
  @ReturnType("java.lang.Long")
  @Deprecated
  public Response getUptimeMs() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mWorkerProcess.getUptimeMs();
      }
    });
  }

  /**
   * @summary get the worker metrics
   * @return the response object
   * @deprecated since version 1.4 and will be removed in version 2.0
   * @see #getInfo(Boolean)
   */
  @GET
  @Path(GET_METRICS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  @Deprecated
  public Response getMetrics() {
    return RestUtils.call(new RestUtils.RestCallable<Map<String, Long>>() {
      @Override
      public Map<String, Long> call() throws Exception {
        return getMetricsInternal();
      }
    });
  }

  private Capacity getCapacityInternal() {
    return new Capacity().setTotal(mStoreMeta.getCapacityBytes())
        .setUsed(mStoreMeta.getUsedBytes());
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

    // Only the gauge for cached blocks is retrieved here, other gauges are statistics of
    // free/used spaces, those statistics can be gotten via other REST apis.
    String blocksCachedProperty =
        MetricsSystem.getWorkerMetricName(DefaultBlockWorker.Metrics.BLOCKS_CACHED);
    @SuppressWarnings("unchecked")
    Gauge<Integer> blocksCached =
        (Gauge<Integer>) metricRegistry.getGauges().get(blocksCachedProperty);

    // Get values of the counters and gauges and put them into a metrics map.
    SortedMap<String, Long> metrics = new TreeMap<>();
    for (Map.Entry<String, Counter> counter : counters.entrySet()) {
      metrics.put(counter.getKey(), counter.getValue().getCount());
    }
    metrics.put(blocksCachedProperty, blocksCached.getValue().longValue());

    return metrics;
  }

  private Comparator<String> getTierAliasComparator() {
    return new Comparator<String>() {
      private WorkerStorageTierAssoc mTierAssoc = new WorkerStorageTierAssoc();

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

  private Map<String, Capacity> getTierCapacityInternal() {
    SortedMap<String, Capacity> tierCapacity = new TreeMap<>(getTierAliasComparator());
    Map<String, Long> capacityBytesOnTiers = mStoreMeta.getCapacityBytesOnTiers();
    Map<String, Long> usedBytesOnTiers = mStoreMeta.getUsedBytesOnTiers();
    for (Map.Entry<String, Long> entry : capacityBytesOnTiers.entrySet()) {
      tierCapacity.put(entry.getKey(),
          new Capacity().setTotal(entry.getValue()).setUsed(usedBytesOnTiers.get(entry.getKey())));
    }
    return tierCapacity;
  }

  private Map<String, List<String>> getTierPathsInternal() {
    SortedMap<String, List<String>> tierToDirPaths = new TreeMap<>(getTierAliasComparator());
    tierToDirPaths.putAll(mStoreMeta.getDirectoryPathsOnTiers());
    return tierToDirPaths;
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
    return RestUtils.call(new RestUtils.RestCallable<LogInfo>() {
      @Override
      public LogInfo call() throws Exception {
        return LogUtils.setLogLevel(logName, level);
      }
    });
  }
}
