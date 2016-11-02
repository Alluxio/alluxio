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

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.PropertyKey;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.metrics.MetricsSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.web.MasterUIWebServer;
import alluxio.wire.WorkerInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.qmino.miredot.annotations.ReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  public static final String SERVICE_PREFIX = "master";
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

  private final AlluxioMaster mMaster;
  private final BlockMaster mBlockMaster;
  private final String mUfsRoot = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
  private final UnderFileSystem mUfs = UnderFileSystem.get(mUfsRoot);

  /**
   * Constructs a new {@link AlluxioMasterRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public AlluxioMasterRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mMaster =
        (AlluxioMaster) context.getAttribute(MasterUIWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY);
    mBlockMaster = mMaster.getBlockMaster();
  }

  /**
   * @summary get the configuration map, the keys are ordered alphabetically.
   * @param raw whether the unresolved, raw property values should be returned, null means false
   * @return the response object
   */
  @GET
  @Path(GET_CONFIGURATION)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.String>")
  public Response getConfiguration(@QueryParam("raw") final Boolean raw) {
    return RestUtils.call(new RestUtils.RestCallable<Map<String, String>>() {
      @Override
      public Map<String, String> call() throws Exception {
        Map<String, String> properties = Configuration.toMap();
        SortedMap<String, String> configuration = new TreeMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          String key = entry.getKey();
          if (PropertyKey.isValid(key)) {
            String value = (raw != null && raw) ? entry.getValue() :
                Configuration.get(PropertyKey.fromString(key));
            configuration.put(key, value);
          }
        }
        return configuration;
      }
    });
  }

  /**
   * @summary get the master rpc address
   * @return the response object
   */
  @GET
  @Path(GET_RPC_ADDRESS)
  @ReturnType("java.lang.String")
  public Response getRpcAddress() {
    return RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return mMaster.getMasterAddress().toString();
      }
    });
  }

  /**
   * @summary get the master metrics, the keys are ordered alphabetically.
   * @return the response object
   */
  @GET
  @Path(GET_METRICS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  public Response getMetrics() {
    return RestUtils.call(new RestUtils.RestCallable<Map<String, Long>>() {
      @Override
      public Map<String, Long> call() throws Exception {
        // Get all counters.
        Map<String, Counter> counters = MetricsSystem.METRIC_REGISTRY.getCounters();

        // Only the gauge for pinned files is retrieved here, other gauges are statistics of
        // free/used
        // spaces, those statistics can be gotten via other REST apis.
        String filesPinnedProperty =
            MetricsSystem.getMasterMetricName(FileSystemMaster.Metrics.FILES_PINNED);
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
    });
  }

  /**
   * @summary get the start time of the master
   * @return the response object
   */
  @GET
  @Path(GET_START_TIME_MS)
  @ReturnType("java.lang.Long")
  public Response getStartTimeMs() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mMaster.getStartTimeMs();
      }
    });
  }

  /**
   * @summary get the uptime of the master
   * @return the response object
   */
  @GET
  @Path(GET_UPTIME_MS)
  @ReturnType("java.lang.Long")
  public Response getUptimeMs() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mMaster.getUptimeMs();
      }
    });
  }

  /**
   * @summary get the version of the master
   * @return the response object
   */
  @GET
  @Path(GET_VERSION)
  @ReturnType("java.lang.String")
  public Response getVersion() {
    return RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return RuntimeConstants.VERSION;
      }
    });
  }

  /**
   * @summary get the total capacity of all workers in bytes
   * @return the response object
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  public Response getCapacityBytes() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mBlockMaster.getCapacityBytes();
      }
    });
  }

  /**
   * @summary get the used capacity
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUsedBytes() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mBlockMaster.getUsedBytes();
      }
    });
  }

  /**
   * @summary get the free capacity
   * @return the response object
   */
  @GET
  @Path(GET_FREE_BYTES)
  @ReturnType("java.lang.Long")
  public Response getFreeBytes() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes();
      }
    });
  }

  /**
   * @summary get the total ufs capacity in bytes, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_UFS_CAPACITY_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUfsCapacityBytes() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL);
      }
    });
  }

  /**
   * @summary get the used disk capacity, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_UFS_USED_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUfsUsedBytes() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_USED);
      }
    });
  }

  /**
   * @summary get the free ufs capacity in bytes, a negative value means the capacity is unknown.
   * @return the response object
   */
  @GET
  @Path(GET_UFS_FREE_BYTES)
  @ReturnType("java.lang.Long")
  public Response getUfsFreeBytes() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_FREE);
      }
    });
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
   */
  @GET
  @Path(GET_CAPACITY_BYTES_ON_TIERS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  public Response getCapacityBytesOnTiers() {
    return RestUtils.call(new RestUtils.RestCallable<Map<String, Long>>() {
      @Override
      public Map<String, Long> call() throws Exception {
        SortedMap<String, Long> capacityBytesOnTiers = new TreeMap<>(getTierAliasComparator());
        for (Map.Entry<String, Long> tierBytes : mBlockMaster.getTotalBytesOnTiers().entrySet()) {
          capacityBytesOnTiers.put(tierBytes.getKey(), tierBytes.getValue());
        }
        return capacityBytesOnTiers;
      }
    });
  }

  /**
   * @summary get the mapping from tier alias to the used bytes of the tier, keys are in the order
   *    from tier alias with smaller ordinal to those with larger ones.
   * @return the response object
   */
  @GET
  @Path(GET_USED_BYTES_ON_TIERS)
  @ReturnType("java.util.SortedMap<java.lang.String, java.lang.Long>")
  public Response getUsedBytesOnTiers() {
    return RestUtils.call(new RestUtils.RestCallable<Map<String, Long>>() {
      @Override
      public Map<String, Long> call() throws Exception {
        SortedMap<String, Long> usedBytesOnTiers = new TreeMap<>(getTierAliasComparator());
        for (Map.Entry<String, Long> tierBytes : mBlockMaster.getUsedBytesOnTiers().entrySet()) {
          usedBytesOnTiers.put(tierBytes.getKey(), tierBytes.getValue());
        }
        return usedBytesOnTiers;
      }
    });
  }

  /**
   * @summary get the count of workers
   * @return the response object
   */
  @GET
  @Path(GET_WORKER_COUNT)
  @ReturnType("java.lang.Integer")
  public Response getWorkerCount() {
    return RestUtils.call(new RestUtils.RestCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return mBlockMaster.getWorkerCount();
      }
    });
  }

  /**
   * @summary get the list of worker descriptors
   * @return the response object
   */
  @GET
  @Path(GET_WORKER_INFO_LIST)
  @ReturnType("java.util.List<alluxio.wire.WorkerInfo>")
  public Response getWorkerInfoList() {
    return RestUtils.call(new RestUtils.RestCallable<List<WorkerInfo>>() {
      @Override
      public List<WorkerInfo> call() throws Exception {
        return mBlockMaster.getWorkerInfoList();
      }
    });
  }
}
