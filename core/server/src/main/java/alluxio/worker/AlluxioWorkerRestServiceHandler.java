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
import alluxio.web.WorkerUIWebServer;
import alluxio.wire.AlluxioWorkerInfo;
import alluxio.wire.Capacity;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.DefaultBlockWorker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
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
  public static final String GET_INFO = "info";

  private final AlluxioWorkerService mWorker;
  private final BlockStoreMeta mStoreMeta;

  /**
   * @param context context for the servlet
   */
  public AlluxioWorkerRestServiceHandler(@Context ServletContext context) {
    mWorker = (AlluxioWorkerService) context
        .getAttribute(WorkerUIWebServer.ALLUXIO_WORKER_SERVLET_RESOURCE_KEY);
    mStoreMeta = mWorker.getBlockWorker().getStoreMeta();
  }

  /**
   * @summary get the Alluxio master information
   * @return the response object
   */
  @GET
  @Path(GET_INFO)
  public Response getInfo() {
    return RestUtils.call(new RestUtils.RestCallable<AlluxioWorkerInfo>() {
      @Override
      public AlluxioWorkerInfo call() throws Exception {
        AlluxioWorkerInfo result =
            new AlluxioWorkerInfo()
                .setCapacity(getCapacity())
                .setConfiguration(getConfiguration())
                .setRpcAddress(mWorker.getRpcAddress().toString())
                .setStartTimeMs(mWorker.getStartTimeMs())
                .setTierCapacity(getTierCapacity())
                .setTierPaths(getTierPaths())
                .setUptimeMs(mWorker.getUptimeMs())
                .setVersion(RuntimeConstants.VERSION);
        return result;
      }
    });
  }

  private Capacity getCapacity() {
    return new Capacity().setTotal(mStoreMeta.getCapacityBytes())
        .setUsed(mStoreMeta.getUsedBytes());
  }

  private Map<String, String> getConfiguration() {
    Set<Map.Entry<String, String>> properties = Configuration.toMap().entrySet();
    SortedMap<String, String> configuration = new TreeMap<>();
    for (Map.Entry<String, String> entry : properties) {
      String key = entry.getKey();
      if (PropertyKey.isValid(key)) {
        configuration.put(key, entry.getValue());
      }
    }
    return configuration;
  }

  private Map<String, Long> getMetrics() {
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

  private Map<String, Capacity> getTierCapacity() {
    SortedMap<String, Capacity> tierCapacity = new TreeMap<>(getTierAliasComparator());
    Map<String, Long> capacityBytesOnTiers = mStoreMeta.getCapacityBytesOnTiers();
    Map<String, Long> usedBytesOnTiers = mStoreMeta.getUsedBytesOnTiers();
    for (Map.Entry<String, Long> entry : capacityBytesOnTiers.entrySet()) {
      tierCapacity.put(entry.getKey(),
          new Capacity().setTotal(entry.getValue()).setUsed(usedBytesOnTiers.get(entry.getKey())));
    }
    return tierCapacity;
  }

  private Map<String, List<String>> getTierPaths() {
    SortedMap<String, List<String>> tierToDirPaths = new TreeMap<>(getTierAliasComparator());
    tierToDirPaths.putAll(mStoreMeta.getDirectoryPathsOnTiers());
    return tierToDirPaths;
  }
}
