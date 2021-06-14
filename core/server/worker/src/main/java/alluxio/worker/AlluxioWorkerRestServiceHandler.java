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

import alluxio.AlluxioURI;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.WorkerStorageTierAssoc;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.block.BlockId;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.FormatUtils;
import alluxio.util.LogUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.webui.UIFileBlockInfo;
import alluxio.util.webui.UIFileInfo;
import alluxio.util.webui.UIStorageDir;
import alluxio.util.webui.UIUsageOnTier;
import alluxio.util.webui.UIWorkerInfo;
import alluxio.util.webui.WebUtils;
import alluxio.web.WorkerWebServer;
import alluxio.wire.AlluxioWorkerInfo;
import alluxio.wire.Capacity;
import alluxio.wire.WorkerWebUIBlockInfo;
import alluxio.wire.WorkerWebUIInit;
import alluxio.wire.WorkerWebUILogs;
import alluxio.wire.WorkerWebUIMetrics;
import alluxio.wire.WorkerWebUIOverview;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.meta.BlockMeta;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.DefaultValue;
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
@Api(value = "/worker", description = "Alluxio Worker Rest Service")
@Path(AlluxioWorkerRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioWorkerRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioWorkerRestServiceHandler.class);

  public static final String SERVICE_PREFIX = "worker";

  // endpoints
  public static final String GET_INFO = "info";

  // webui endpoints // TODO(william): DRY up these enpoints
  public static final String WEBUI_INIT = "webui_init";
  public static final String WEBUI_OVERVIEW = "webui_overview";
  public static final String WEBUI_LOGS = "webui_logs";
  public static final String WEBUI_BLOCKINFO = "webui_blockinfo";
  public static final String WEBUI_METRICS = "webui_metrics";

  // queries
  public static final String QUERY_RAW_CONFIGURATION = "raw_configuration";

  // log
  public static final String LOG_LEVEL = "logLevel";
  public static final String LOG_ARGUMENT_NAME = "logName";
  public static final String LOG_ARGUMENT_LEVEL = "level";

  private final WorkerProcess mWorkerProcess;
  private final BlockStoreMeta mStoreMeta;
  private final BlockWorker mBlockWorker;
  private final FileSystem mFsClient;

  /**
   * @param context context for the servlet
   */
  public AlluxioWorkerRestServiceHandler(@Context ServletContext context) {
    mWorkerProcess =
        (WorkerProcess) context.getAttribute(WorkerWebServer.ALLUXIO_WORKER_SERVLET_RESOURCE_KEY);
    mBlockWorker = mWorkerProcess.getWorker(BlockWorker.class);
    mStoreMeta = mWorkerProcess.getWorker(BlockWorker.class).getStoreMeta();
    mFsClient =
        (FileSystem) context.getAttribute(WorkerWebServer.ALLUXIO_FILESYSTEM_CLIENT_RESOURCE_KEY);
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
  @ApiOperation(value = "Get general Alluxio Worker service information",
      response = alluxio.wire.AlluxioWorkerInfo.class)
  public Response getInfo(@QueryParam(QUERY_RAW_CONFIGURATION) final Boolean rawConfiguration) {
    // TODO(jiri): Add a mechanism for retrieving only a subset of the fields.
    return RestUtils.call(() -> {
      boolean rawConfig = false;
      if (rawConfiguration != null) {
        rawConfig = rawConfiguration;
      }
      AlluxioWorkerInfo result = new AlluxioWorkerInfo().setCapacity(getCapacityInternal())
          .setConfiguration(getConfigurationInternal(rawConfig)).setMetrics(getMetricsInternal())
          .setRpcAddress(mWorkerProcess.getRpcAddress().toString())
          .setStartTimeMs(mWorkerProcess.getStartTimeMs())
          .setTierCapacity(getTierCapacityInternal()).setTierPaths(getTierPathsInternal())
          .setUptimeMs(mWorkerProcess.getUptimeMs()).setVersion(RuntimeConstants.VERSION);
      return result;
    }, ServerConfiguration.global());
  }

  /**
   * Gets Web UI initialization data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_INIT)
  public Response getWebUIInit() {
    return RestUtils.call(() -> {
      WorkerWebUIInit response = new WorkerWebUIInit();

      response.setDebug(ServerConfiguration.getBoolean(PropertyKey.DEBUG))
          .setWebFileInfoEnabled(ServerConfiguration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED))
          .setSecurityAuthorizationPermissionEnabled(
              ServerConfiguration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED))
          .setMasterHostname(NetworkAddressUtils
              .getConnectHost(NetworkAddressUtils.ServiceType.MASTER_WEB,
                  ServerConfiguration.global()))
          .setMasterPort(ServerConfiguration.getInt(PropertyKey.MASTER_WEB_PORT))
          .setRefreshInterval((int) ServerConfiguration.getMs(PropertyKey.WEB_REFRESH_INTERVAL));

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets web ui overview page data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_OVERVIEW)
  public Response getWebUIOverview() {
    return RestUtils.call(() -> {
      WorkerWebUIOverview response = new WorkerWebUIOverview();

      response.setWorkerInfo(new UIWorkerInfo(mWorkerProcess.getRpcAddress().toString(),
          mWorkerProcess.getStartTimeMs(),
          ServerConfiguration.get(PropertyKey.USER_DATE_FORMAT_PATTERN)));

      BlockStoreMeta storeMeta = mBlockWorker.getStoreMeta();
      long capacityBytes = 0L;
      long usedBytes = 0L;
      Map<String, Long> capacityBytesOnTiers = storeMeta.getCapacityBytesOnTiers();
      Map<String, Long> usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
      List<UIUsageOnTier> usageOnTiers = new ArrayList<>();
      for (Map.Entry<String, Long> entry : capacityBytesOnTiers.entrySet()) {
        String tier = entry.getKey();
        long capacity = entry.getValue();
        Long nullableUsed = usedBytesOnTiers.get(tier);
        long used = nullableUsed == null ? 0 : nullableUsed;

        capacityBytes += capacity;
        usedBytes += used;

        usageOnTiers.add(new UIUsageOnTier(tier, capacity, used));
      }

      response.setCapacityBytes(FormatUtils.getSizeFromBytes(capacityBytes))
          .setUsedBytes(FormatUtils.getSizeFromBytes(usedBytes)).setUsageOnTiers(usageOnTiers)
          .setVersion(RuntimeConstants.VERSION);

      List<UIStorageDir> storageDirs = new ArrayList<>(storeMeta.getCapacityBytesOnDirs().size());
      for (Pair<String, String> tierAndDirPath : storeMeta.getCapacityBytesOnDirs().keySet()) {
        storageDirs.add(new UIStorageDir(tierAndDirPath.getFirst(), tierAndDirPath.getSecond(),
            storeMeta.getCapacityBytesOnDirs().get(tierAndDirPath),
            storeMeta.getUsedBytesOnDirs().get(tierAndDirPath)));
      }

      response.setStorageDirs(storageDirs);

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets web ui block info page data.
   *
   * @param requestPath the request path
   * @param requestOffset the request offset
   * @param requestLimit the request limit
   * @return the response object
   */
  @GET
  @Path(WEBUI_BLOCKINFO)
  public Response getWebUIBlockInfo(@QueryParam("path") String requestPath,
      @DefaultValue("0") @QueryParam("offset") String requestOffset,
      @DefaultValue("20") @QueryParam("limit") String requestLimit) {
    return RestUtils.call(() -> {
      WorkerWebUIBlockInfo response = new WorkerWebUIBlockInfo();

      if (!ServerConfiguration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return response;
      }
      response.setFatalError("").setInvalidPathError("");
      if (!(requestPath == null || requestPath.isEmpty())) {
        // Display file block info
        try {
          URIStatus status = mFsClient.getStatus(new AlluxioURI(requestPath));
          UIFileInfo uiFileInfo = new UIFileInfo(status, ServerConfiguration.global(),
              new WorkerStorageTierAssoc().getOrderedStorageAliases());
          for (long blockId : status.getBlockIds()) {
            if (mBlockWorker.hasBlockMeta(blockId)) {
              BlockMeta blockMeta = mBlockWorker.getVolatileBlockMeta(blockId);
              long blockSize = blockMeta.getBlockSize();
              // The block last access time is not available. Use -1 for now.
              // It's not necessary to show location information here since
              // we are viewing at the context of this worker.
              uiFileInfo.addBlock(blockMeta.getBlockLocation().tierAlias(), blockId, blockSize, -1);
            }
          }
          List<ImmutablePair<String, List<UIFileBlockInfo>>> fileBlocksOnTier = new ArrayList<>();
          for (Map.Entry<String, List<UIFileBlockInfo>> e : uiFileInfo.getBlocksOnTier()
              .entrySet()) {
            fileBlocksOnTier.add(new ImmutablePair<>(e.getKey(), e.getValue()));
          }

          response.setFileBlocksOnTier(fileBlocksOnTier)
              .setBlockSizeBytes(uiFileInfo.getBlockSizeBytes()).setPath(requestPath);
        } catch (FileDoesNotExistException e) {
          response.setFatalError("Error: Invalid Path " + e.getMessage());
        } catch (IOException e) {
          response.setInvalidPathError(
              "Error: File " + requestPath + " is not available " + e.getMessage());
        } catch (BlockDoesNotExistException e) {
          response.setFatalError("Error: block not found. " + e.getMessage());
        } catch (AlluxioException e) {
          response.setFatalError("Error: alluxio exception. " + e.getMessage());
        }
      }

      Set<Long> unsortedFileIds = new HashSet<>();
      BlockStoreMeta storeMeta = mBlockWorker.getStoreMetaFull();
      for (List<Long> blockIds : storeMeta.getBlockList().values()) {
        for (long blockId : blockIds) {
          unsortedFileIds.add(BlockId.getFileId(blockId));
        }
      }
      List<Long> fileIds = new ArrayList<>(unsortedFileIds);
      Collections.sort(fileIds);
      response.setNTotalFile(unsortedFileIds.size())
          .setOrderedTierAliases(new WorkerStorageTierAssoc().getOrderedStorageAliases());

      try {
        int offset = Integer.parseInt(requestOffset);
        int limit = Integer.parseInt(requestLimit);
        // make the limit the total number of files if request limit is > than what is available
        limit = offset == 0 && limit > fileIds.size() ? fileIds.size() : limit;
        // offset+limit can't be greater than the size of the list
        limit = offset + limit > fileIds.size() ? fileIds.size() - offset : limit;
        int sum = Math.addExact(offset, limit);
        List<Long> subFileIds = fileIds.subList(offset, sum);
        List<UIFileInfo> uiFileInfos = new ArrayList<>(subFileIds.size());
        for (long fileId : subFileIds) {
          try {
            URIStatus status = new URIStatus(mBlockWorker.getFileInfo(fileId));
            UIFileInfo uiFileInfo = new UIFileInfo(status, ServerConfiguration.global(),
                new WorkerStorageTierAssoc().getOrderedStorageAliases());
            for (long blockId : status.getBlockIds()) {
              if (mBlockWorker.hasBlockMeta(blockId)) {
                BlockMeta blockMeta = mBlockWorker.getVolatileBlockMeta(blockId);
                long blockSize = blockMeta.getBlockSize();
                // The block last access time is not available. Use -1 for now.
                // It's not necessary to show location information here since
                // we are viewing at the context of this worker.
                uiFileInfo
                    .addBlock(blockMeta.getBlockLocation().tierAlias(), blockId, blockSize, -1);
              }
            }
            if (!uiFileInfo.getBlockIds().isEmpty()) {
              uiFileInfos.add(uiFileInfo);
            }
          } catch (Exception e) {
            // The file might have been deleted, log a warning and ignore this file.
            LOG.warn("Unable to get file info for fileId {}. {}", fileId, e.toString());
          }
        }
        response.setFileInfos(uiFileInfos);
      } catch (NumberFormatException e) {
        response.setFatalError("Error: offset or limit parse error, " + e.getLocalizedMessage());
      } catch (ArithmeticException e) {
        response.setFatalError(
            "Error: offset or offset + limit is out ofbound, " + e.getLocalizedMessage());
      } catch (Exception e) {
        response.setFatalError(e.getLocalizedMessage());
      }

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets web ui metrics page data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_METRICS)
  public Response getWebUIMetrics() {
    return RestUtils.call(() -> {
      WorkerWebUIMetrics response = new WorkerWebUIMetrics();

      MetricRegistry mr = MetricsSystem.METRIC_REGISTRY;
      SortedMap<String, Gauge> gauges = mr.getGauges();

      Long workerCapacityTotal = (Long) gauges.get(MetricsSystem
          .getMetricName(MetricKey.WORKER_CAPACITY_TOTAL.getName())).getValue();
      Long workerCapacityUsed = (Long) gauges.get(MetricsSystem
          .getMetricName(MetricKey.WORKER_CAPACITY_USED.getName())).getValue();

      int workerCapacityUsedPercentage =
          (workerCapacityTotal > 0) ? (int) (100L * workerCapacityUsed / workerCapacityTotal) : 0;

      response.setWorkerCapacityUsedPercentage(workerCapacityUsedPercentage);
      response.setWorkerCapacityFreePercentage(100L - workerCapacityUsedPercentage);

      Map<String, Metric> operations = new TreeMap<>();
      // Remove the instance name from the metrics.
      for (Map.Entry<String, Counter> entry : mr.getCounters().entrySet()) {
        operations.put(MetricsSystem.stripInstanceAndHost(entry.getKey()), entry.getValue());
      }

      response.setOperationMetrics(operations);

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets web ui logs page data.
   *
   * @param requestPath the request path
   * @param requestOffset the request offset
   * @param requestEnd the request end
   * @param requestLimit the request limit
   * @return the response object
   */
  @GET
  @Path(WEBUI_LOGS)
  public Response getWebUILogs(@DefaultValue("") @QueryParam("path") String requestPath,
      @DefaultValue("0") @QueryParam("offset") String requestOffset,
      @QueryParam("end") String requestEnd,
      @DefaultValue("20") @QueryParam("limit") String requestLimit) {
    return RestUtils.call(() -> {
      FilenameFilter filenameFilter = (dir, name) -> name.toLowerCase().endsWith(".log");
      WorkerWebUILogs response = new WorkerWebUILogs();

      if (!ServerConfiguration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return response;
      }
      response.setDebug(ServerConfiguration.getBoolean(PropertyKey.DEBUG)).setInvalidPathError("")
          .setViewingOffset(0).setCurrentPath("");
      //response.setDownloadLogFile(1);
      //response.setBaseUrl("./browseLogs");
      //response.setShowPermissions(false);

      String logsPath = ServerConfiguration.get(PropertyKey.LOGS_DIR);
      File logsDir = new File(logsPath);
      String requestFile = requestPath;

      if (requestFile == null || requestFile.isEmpty()) {
        // List all log files in the log/ directory.

        List<UIFileInfo> fileInfos = new ArrayList<>();
        File[] logFiles = logsDir.listFiles(filenameFilter);
        if (logFiles != null) {
          for (File logFile : logFiles) {
            String logFileName = logFile.getName();
            fileInfos.add(new UIFileInfo(
                new UIFileInfo.LocalFileInfo(logFileName, logFileName, logFile.length(),
                    UIFileInfo.LocalFileInfo.EMPTY_CREATION_TIME, logFile.lastModified(),
                    logFile.isDirectory()), ServerConfiguration.global(),
                new WorkerStorageTierAssoc().getOrderedStorageAliases()));
          }
        }
        Collections.sort(fileInfos, UIFileInfo.PATH_STRING_COMPARE);
        response.setNTotalFile(fileInfos.size());

        try {
          int offset = Integer.parseInt(requestOffset);
          int limit = Integer.parseInt(requestLimit);
          limit = offset == 0 && limit > fileInfos.size() ? fileInfos.size() : limit;
          limit = offset + limit > fileInfos.size() ? fileInfos.size() - offset : limit;
          int sum = Math.addExact(offset, limit);
          fileInfos = fileInfos.subList(offset, sum);
          response.setFileInfos(fileInfos);
        } catch (NumberFormatException e) {
          response.setFatalError("Error: offset or limit parse error, " + e.getLocalizedMessage());
          return response;
        } catch (ArithmeticException e) {
          response.setFatalError(
              "Error: offset or offset + limit is out of bound, " + e.getLocalizedMessage());
          return response;
        } catch (IllegalArgumentException e) {
          response.setFatalError(e.getLocalizedMessage());
          return response;
        }
      } else {
        // Request a specific log file.

        // Only allow filenames as the path, to avoid arbitrary local path lookups.
        requestFile = new File(requestFile).getName();
        response.setCurrentPath(requestFile);

        File logFile = new File(logsDir, requestFile);

        try {
          long fileSize = logFile.length();
          String offsetParam = requestOffset;
          long relativeOffset = 0;
          long offset;
          try {
            if (offsetParam != null) {
              relativeOffset = Long.parseLong(offsetParam);
            }
          } catch (NumberFormatException e) {
            relativeOffset = 0;
          }
          String endParam = requestEnd;
          // If no param "end" presents, the offset is relative to the beginning; otherwise, it is
          // relative to the end of the file.
          if (endParam == null) {
            offset = relativeOffset;
          } else {
            offset = fileSize - relativeOffset;
          }
          if (offset < 0) {
            offset = 0;
          } else if (offset > fileSize) {
            offset = fileSize;
          }

          String fileData;
          try (InputStream is = new FileInputStream(logFile)) {
            fileSize = logFile.length();
            int len = (int) Math.min(5L * Constants.KB, fileSize - offset);
            byte[] data = new byte[len];
            long skipped = is.skip(offset);
            if (skipped < 0) {
              // Nothing was skipped.
              fileData = "Unable to traverse to offset; is file empty?";
            } else if (skipped < offset) {
              // Couldn't skip all the way to offset.
              fileData = "Unable to traverse to offset; is offset larger than the file?";
            } else {
              // Read may not read up to len, so only convert what was read.
              int read = is.read(data, 0, len);
              if (read < 0) {
                // Stream couldn't read anything, skip went to EOF?
                fileData = "Unable to read file";
              } else {
                fileData = WebUtils.convertByteArrayToStringWithoutEscape(data, 0, read);
              }
            }
          }
          response.setFileData(fileData).setViewingOffset(offset);
        } catch (IOException e) {
          response.setInvalidPathError(
              "Error: File " + logFile + " is not available " + e.getMessage());
        }
      }

      return response;
    }, ServerConfiguration.global());
  }

  private Capacity getCapacityInternal() {
    return new Capacity().setTotal(mStoreMeta.getCapacityBytes())
        .setUsed(mStoreMeta.getUsedBytes());
  }

  private Map<String, String> getConfigurationInternal(boolean raw) {
    return new TreeMap<>(ServerConfiguration
        .toMap(ConfigurationValueOptions.defaults().useDisplayValue(true).useRawValue(raw)));
  }

  private Map<String, Long> getMetricsInternal() {
    MetricRegistry metricRegistry = MetricsSystem.METRIC_REGISTRY;

    // Get all counters.
    Map<String, Counter> counters = metricRegistry.getCounters();

    // Only the gauge for cached blocks is retrieved here, other gauges are statistics of
    // free/used spaces, those statistics can be gotten via other REST apis.
    String blocksCachedProperty =
        MetricsSystem.getMetricName(MetricKey.WORKER_BLOCKS_CACHED.getName());
    @SuppressWarnings("unchecked") Gauge<Integer> blocksCached =
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
        return Integer.compare(ordinal1, ordinal2);
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
  public Response logLevel(@QueryParam(LOG_ARGUMENT_NAME) final String logName,
      @QueryParam(LOG_ARGUMENT_LEVEL) final String level) {
    return RestUtils.call(() -> LogUtils.setLogLevel(logName, level), ServerConfiguration.global());
  }
}
