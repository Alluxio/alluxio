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
import alluxio.PropertyKey;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.metrics.MetricsSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.web.MasterUIWebServer;
import alluxio.wire.AlluxioMasterInfo;
import alluxio.wire.Capacity;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
 * This class is a REST handler for requesting general master information.
 */
@NotThreadSafe
@Path(AlluxioMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioMasterRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  public static final String SERVICE_PREFIX = "master";
  public static final String GET_INFO = "info";

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
   * @summary get the Alluxio master information
   * @return the response object
   */
  @GET
  @Path(GET_INFO)
  public Response getInfo() {
    return RestUtils.call(new RestUtils.RestCallable<AlluxioMasterInfo>() {
      @Override
      public AlluxioMasterInfo call() throws Exception {
        AlluxioMasterInfo result =
            new AlluxioMasterInfo()
                .setCapacity(getCapacity())
                .setConfiguration(getConfiguration())
                .setMetrics(getMetrics())
                .setRpcAddress(mMaster.getMasterAddress().toString())
                .setStartTimeMs(mMaster.getStartTimeMs())
                .setTierCapacity(getTierCapacity())
                .setUfsCapacity(getUfsCapacity())
                .setUptimeMs(mMaster.getUptimeMs())
                .setVersion(RuntimeConstants.VERSION)
                .setWorkers(mBlockMaster.getWorkerInfoList());
        return result;
      }
    });
  }

  private Capacity getCapacity() {
    return new Capacity().setTotal(mBlockMaster.getCapacityBytes())
        .setUsed(mBlockMaster.getUsedBytes());
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

  private Map<String, Capacity> getTierCapacity() {
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

  private Capacity getUfsCapacity() throws IOException {
    return new Capacity().setTotal(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL))
        .setUsed(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_USED));
  }
}
