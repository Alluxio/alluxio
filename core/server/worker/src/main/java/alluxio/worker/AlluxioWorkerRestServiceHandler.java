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
import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.Bits;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.master.block.BlockId;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.s3.ChunkedEncodingInputStream;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;
import alluxio.s3.S3RangeSpec;
import alluxio.s3.TaggingData;
import alluxio.security.User;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.LogUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.webui.UIFileBlockInfo;
import alluxio.util.webui.UIFileInfo;
import alluxio.util.webui.UIStorageDir;
import alluxio.util.webui.UIUsageOnTier;
import alluxio.util.webui.UIWorkerInfo;
import alluxio.util.webui.WebUtils;
import alluxio.web.RangeFileInStream;
import alluxio.web.WorkerWebServer;
import alluxio.wire.AlluxioWorkerInfo;
import alluxio.wire.Capacity;
import alluxio.wire.WorkerWebUIBlockInfo;
import alluxio.wire.WorkerWebUIConfiguration;
import alluxio.wire.WorkerWebUIInit;
import alluxio.wire.WorkerWebUILogs;
import alluxio.wire.WorkerWebUIMetrics;
import alluxio.wire.WorkerWebUIOverview;
import alluxio.worker.block.BlockStoreMeta;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
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
  public static final String WEBUI_CONFIG = "webui_config";

  // queries
  public static final String QUERY_RAW_CONFIGURATION = "raw_configuration";

  // log
  public static final String LOG_LEVEL = "logLevel";
  public static final String LOG_ARGUMENT_NAME = "logName";
  public static final String LOG_ARGUMENT_LEVEL = "level";

  private final WorkerProcess mWorkerProcess;
  private final BlockStoreMeta mStoreMeta;
  private final DefaultBlockWorker mBlockWorker;
  private final FileSystem mFsClient;
  private final int mMaxHeaderMetadataSize;

  /**
   * @param context context for the servlet
   */
  public AlluxioWorkerRestServiceHandler(@Context ServletContext context) {
    mWorkerProcess =
        (WorkerProcess) context.getAttribute(WorkerWebServer.ALLUXIO_WORKER_SERVLET_RESOURCE_KEY);
    mBlockWorker = (DefaultBlockWorker) mWorkerProcess.getWorker(BlockWorker.class);
    mStoreMeta = mBlockWorker.getStoreMeta();
    mFsClient =
        (FileSystem) context.getAttribute(WorkerWebServer.ALLUXIO_FILESYSTEM_CLIENT_RESOURCE_KEY);
    mMaxHeaderMetadataSize = (int) Configuration.getBytes(
        PropertyKey.PROXY_S3_METADATA_HEADER_MAX_SIZE);
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
      return new AlluxioWorkerInfo().setCapacity(getCapacityInternal())
          .setConfiguration(getConfigurationInternal(rawConfig)).setMetrics(getMetricsInternal())
          .setRpcAddress(mWorkerProcess.getRpcAddress().toString())
          .setStartTimeMs(mWorkerProcess.getStartTimeMs())
          .setTierCapacity(getTierCapacityInternal()).setTierPaths(getTierPathsInternal())
          .setUptimeMs(mWorkerProcess.getUptimeMs())
          .setVersion(RuntimeConstants.VERSION)
          .setRevision(ProjectConstants.REVISION);
    }, Configuration.global());
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

      response.setDebug(Configuration.getBoolean(PropertyKey.DEBUG))
          .setWebFileInfoEnabled(Configuration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED))
          .setSecurityAuthorizationPermissionEnabled(
              Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED))
          .setMasterHostname(NetworkAddressUtils
              .getConnectHost(NetworkAddressUtils.ServiceType.MASTER_WEB,
                  Configuration.global()))
          .setMasterPort(Configuration.getInt(PropertyKey.MASTER_WEB_PORT))
          .setRefreshInterval((int) Configuration.getMs(PropertyKey.WEB_REFRESH_INTERVAL));

      return response;
    }, Configuration.global());
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
          Configuration.getString(PropertyKey.USER_DATE_FORMAT_PATTERN)));
      BlockStoreMeta storeMeta = mBlockWorker.getStoreMetaFull();
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
          .setBlockCount(Long.toString(storeMeta.getNumberOfBlocks()))
          .setVersion(RuntimeConstants.VERSION)
          .setRevision(ProjectConstants.REVISION);

      List<UIStorageDir> storageDirs = new ArrayList<>(storeMeta.getCapacityBytesOnDirs().size());
      for (Pair<String, String> tierAndDirPath : storeMeta.getCapacityBytesOnDirs().keySet()) {
        storageDirs.add(new UIStorageDir(tierAndDirPath.getFirst(), tierAndDirPath.getSecond(),
            storeMeta.getCapacityBytesOnDirs().get(tierAndDirPath),
            storeMeta.getUsedBytesOnDirs().get(tierAndDirPath)));
      }

      response.setStorageDirs(storageDirs);

      return response;
    }, Configuration.global());
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

      if (!Configuration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return response;
      }
      response.setFatalError("").setInvalidPathError("");
      if (!(requestPath == null || requestPath.isEmpty())) {
        // Display file block info
        try {
          URIStatus status = mFsClient.getStatus(new AlluxioURI(requestPath));
          UIFileInfo uiFileInfo = new UIFileInfo(status, Configuration.global(),
              mStoreMeta.getStorageTierAssoc().getOrderedStorageAliases());
          for (long blockId : status.getBlockIds()) {
            // The block last access time is not available. Use -1 for now.
            // It's not necessary to show location information here since
            // we are viewing at the context of this worker.
            mBlockWorker.getBlockStore().getVolatileBlockMeta(blockId)
                .ifPresent(meta -> uiFileInfo.addBlock(meta.getBlockLocation().tierAlias(),
                    blockId, meta.getBlockSize(), -1));
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
        } catch (AlluxioException e) {
          response.setFatalError("Error: alluxio exception. " + e.getMessage());
        }
        catch (AlluxioRuntimeException e) {
          response.setFatalError("Error: alluxio run time exception. " + e.getMessage());
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
          .setOrderedTierAliases(mStoreMeta.getStorageTierAssoc().getOrderedStorageAliases());

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
            UIFileInfo uiFileInfo = new UIFileInfo(status, Configuration.global(),
                mStoreMeta.getStorageTierAssoc().getOrderedStorageAliases());
            for (long blockId : status.getBlockIds()) {
              // The block last access time is not available. Use -1 for now.
              // It's not necessary to show location information here since
              // we are viewing at the context of this worker.
              mBlockWorker.getBlockStore().getVolatileBlockMeta(blockId)
                  .ifPresent(meta -> uiFileInfo.addBlock(meta.getBlockLocation().tierAlias(),
                      blockId, meta.getBlockSize(), -1));
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
    }, Configuration.global());
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
    }, Configuration.global());
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

      if (!Configuration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return response;
      }
      response.setDebug(Configuration.getBoolean(PropertyKey.DEBUG)).setInvalidPathError("")
          .setViewingOffset(0).setCurrentPath("");

      String logsPath = Configuration.getString(PropertyKey.LOGS_DIR);
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
                    logFile.isDirectory()), Configuration.global(),
                mStoreMeta.getStorageTierAssoc().getOrderedStorageAliases()));
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
          long relativeOffset = 0;
          long offset;
          try {
            if (requestOffset != null) {
              relativeOffset = Long.parseLong(requestOffset);
            }
          } catch (NumberFormatException e) {
            // pass
          }
          // If no param "end" presents, the offset is relative to the beginning; otherwise, it is
          // relative to the end of the file.
          if (requestEnd == null) {
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
    }, Configuration.global());
  }

  /**
   * Gets Web UI Configuration page data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_CONFIG)
  public Response getWebUIConfiguration() {
    return RestUtils.call(() -> {
      WorkerWebUIConfiguration response = new WorkerWebUIConfiguration();
      response.setWhitelist(mBlockWorker.getWhiteList());

      TreeSet<Triple<String, String, String>> sortedProperties = new TreeSet<>();
      Set<String> alluxioConfExcludes = Sets.newHashSet(PropertyKey.WORKER_WHITELIST.toString());
      for (ConfigProperty configProperty : mBlockWorker
              .getConfiguration(GetConfigurationPOptions.newBuilder().setRawValue(true).build())
              .toProto().getClusterConfigsList()) {
        String confName = configProperty.getName();
        if (!alluxioConfExcludes.contains(confName)) {
          sortedProperties.add(new ImmutableTriple<>(confName,
                  ConfigurationUtils.valueAsString(configProperty.getValue()),
                  configProperty.getSource()));
        }
      }

      response.setConfiguration(sortedProperties);

      return response;
    }, Configuration.global());
  }

  /**
   * read file from worker.
   * @param requestPath
   * @param user
   * @param range
   * @return the response object
   */
  @GET
  @Path("openfile")
  @Produces({MediaType.APPLICATION_OCTET_STREAM,
      MediaType.APPLICATION_XML, MediaType.WILDCARD})
  public Response getFile(@QueryParam("path") String requestPath,
                          @QueryParam("username") String user,
                          @QueryParam("range") String range) {
    return RestUtils.s3call(requestPath, () -> {
      FileSystem userFs = createFileSystemForUser(user, mFsClient);
      URIStatus status = userFs.getStatus(new AlluxioURI(requestPath));
      FileInStream is = userFs.openFile(status, OpenFilePOptions.getDefaultInstance());
      S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
      RangeFileInStream ris = RangeFileInStream.Factory.create(is, status.getLength(), s3Range);

      Response.ResponseBuilder res = Response.ok(ris)
          .lastModified(new Date(status.getLastModificationTimeMs()))
          .header("Content-Length",
              s3Range.getLength(status.getLength()));

      // Check range
      if (s3Range.isValid()) {
        res.status(Response.Status.PARTIAL_CONTENT)
            .header(S3Constants.S3_ACCEPT_RANGES_HEADER, S3Constants.S3_ACCEPT_RANGES_VALUE)
            .header(S3Constants.S3_CONTENT_RANGE_HEADER,
                s3Range.getRealRange(status.getLength()));
      }

      // Check for the object's ETag
      String entityTag = RestUtils.getEntityTag(status);
      if (entityTag != null) {
        res.header(S3Constants.S3_ETAG_HEADER, entityTag);
      } else {
        LOG.debug("Failed to find ETag for object: " + requestPath);
      }

      // Check if the object had a specified "Content-Type"
      res.type(deserializeContentType(status.getXAttr()));

      // Check if object had tags, if so we need to return the count
      // in the header "x-amz-tagging-count"
//      TaggingData tagData = S3RestUtils.deserializeTags(status.getXAttr());
//      if (tagData != null) {
//        int taggingCount = tagData.getTagMap().size();
//        if (taggingCount > 0) {
//          res.header(S3Constants.S3_TAGGING_COUNT_HEADER, taggingCount);
//        }
//      }
      return res.build();
    });
  }

  /**
   * read file from worker.
   * @param requestPath
   * @param user
   * @param contentMD5 the optional Base64 encoded 128-bit MD5 digest of the object
   * @param copySourceParam the URL-encoded source path to copy the new file from
   * @param decodedLength the length of the content when in aws-chunked encoding
   * @param contentLength the total length of the request body
   * @param contentTypeParam the content type of the request body
   * @param tagging query string to indicate if this is for PutObjectTagging or not
   * @param taggingHeader the URL-encoded user tags passed in the header
   * @param is the request body
   * @return the response object
   */
  @PUT
  @Path("writefile")
  @Produces({MediaType.APPLICATION_OCTET_STREAM,
      MediaType.APPLICATION_XML, MediaType.WILDCARD})
  public Response putFile(@QueryParam("path") String requestPath,
                          @QueryParam("username") String user,
                          @HeaderParam("Content-MD5") final String contentMD5,
                          @HeaderParam(S3Constants.S3_COPY_SOURCE_HEADER)
                            final String copySourceParam,
                          @HeaderParam("x-amz-decoded-content-length")
                            final String decodedLength,
                          @HeaderParam(S3Constants.S3_TAGGING_HEADER)
                            final String taggingHeader,
                          @HeaderParam(S3Constants.S3_CONTENT_TYPE_HEADER)
                            final String contentTypeParam,
                          @HeaderParam("Content-Length")
                            final String contentLength,
                          @QueryParam("tagging") final String tagging,
                          final InputStream is) {
    return RestUtils.s3call(requestPath, () -> {
      MessageDigest md5 = MessageDigest.getInstance("MD5");

      // The request body can be in the aws-chunked encoding format, or not encoded at all
      // determine if it's encoded, and then which parts of the stream to read depending on
      // the encoding type.
      boolean isChunkedEncoding = decodedLength != null;
      long toRead;
      InputStream readStream = is;
      if (isChunkedEncoding) {
        toRead = Long.parseLong(decodedLength);
        readStream = new ChunkedEncodingInputStream(is);
      } else {
        toRead = Long.parseLong(contentLength);
      }
      TaggingData tagData = null;
      if (tagging != null) { // PutObjectTagging
        try {
          tagData = new XmlMapper().readerFor(TaggingData.class).readValue(is);
        } catch (IOException e) {
          if (e.getCause() instanceof S3Exception) {
            throw toObjectS3Exception((S3Exception) e.getCause(), requestPath);
          }
          throw new S3Exception(e, requestPath, S3ErrorCode.MALFORMED_XML);
        }
      }
      if (taggingHeader != null) { // Parse the tagging header if it exists for PutObject
        try {
          tagData = deserializeTaggingHeader(taggingHeader, mMaxHeaderMetadataSize);
        } catch (IllegalArgumentException e) {
          if (e.getCause() instanceof S3Exception) {
            throw toObjectS3Exception((S3Exception) e.getCause(), requestPath);
          }
          throw toObjectS3Exception(e, requestPath);
        }
      }
      // Populate the xattr Map with the metadata tags if provided
      Map<String, ByteString> xattrMap = new HashMap<>();
      if (tagData != null) {
        try {
          xattrMap.put(S3Constants.TAGGING_XATTR_KEY, TaggingData.serialize(tagData));
        } catch (Exception e) {
          throw toObjectS3Exception(e, requestPath);
        }
      }

      AlluxioURI objectUri = new AlluxioURI(requestPath);
      CreateFilePOptions filePOptions =
          CreateFilePOptions.newBuilder()
              .setRecursive(true)
              .setMode(PMode.newBuilder()
                  .setOwnerBits(Bits.ALL)
                  .setGroupBits(Bits.ALL)
                  .setOtherBits(Bits.NONE).build())
              .setWriteType(
                  Configuration.getEnum(PropertyKey.PROXY_S3_WRITE_TYPE, WriteType.class).toProto())
//              .putAllXattr(xattrMap)
              .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
              .setOverwrite(true)
              .build();
      FileSystem userFs = createFileSystemForUser(user, mFsClient);
      FileOutStream os = userFs.createFile(objectUri, filePOptions);
      try (DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5)) {
        long read = ByteStreams.copy(ByteStreams.limit(readStream, toRead),
            digestOutputStream);
        if (read < toRead) {
          throw new IOException(String.format(
              "Failed to read all required bytes from the stream. Read %d/%d",
              read, toRead));
        }
      }

      byte[] digest = md5.digest();
      String base64Digest = BaseEncoding.base64().encode(digest);
      if (contentMD5 != null && !contentMD5.equals(base64Digest)) {
        // The object may be corrupted, delete the written object and return an error.
        try {
          userFs.delete(objectUri, DeletePOptions.newBuilder().setRecursive(true).build());
        } catch (Exception e2) {
          // intend to continue and return BAD_DIGEST S3Exception.
        }
        throw new S3Exception(objectUri.getPath(), S3ErrorCode.BAD_DIGEST);
      }

      String entityTag = Hex.encodeHexString(digest);
      // persist the ETag via xAttr
      // TODO(czhu): try to compute the ETag prior to creating the file
      //  to reduce total RPC RTT
      setEntityTag(userFs, objectUri, entityTag);
//      if (partNumber != null) { // UploadPart
//        return Response.ok().header(S3Constants.S3_ETAG_HEADER, entityTag).build();
//      }
      // PutObject
      return Response.ok().header(S3Constants.S3_ETAG_HEADER, entityTag).build();
    });
  }

  private static MediaType deserializeContentType(Map<String, byte[]> xAttr) {
    MediaType type = MediaType.APPLICATION_OCTET_STREAM_TYPE;
    // Fetch the Content-Type from the Inode xAttr
    if (xAttr == null) {
      return type;
    }
    if (xAttr.containsKey("s3_content_type")) {
      String contentType = new String(xAttr.get(
          "s3_content_type"), StandardCharsets.UTF_8);
      if (!contentType.isEmpty()) {
        type = MediaType.valueOf(contentType);
      }
    }
    return type;
  }

  private Capacity getCapacityInternal() {
    return new Capacity().setTotal(mStoreMeta.getCapacityBytes())
        .setUsed(mStoreMeta.getUsedBytes());
  }

  private Map<String, Object> getConfigurationInternal(boolean raw) {
    return new TreeMap<>(Configuration
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
    return (tier1, tier2) -> {
      int ordinal1 = mStoreMeta.getStorageTierAssoc().getOrdinal(tier1);
      int ordinal2 = mStoreMeta.getStorageTierAssoc().getOrdinal(tier2);
      return Integer.compare(ordinal1, ordinal2);
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
    return RestUtils.call(() -> LogUtils.setLogLevel(logName, level), Configuration.global());
  }

  /**
   * @param user the {@link Subject} name of the filesystem user
   * @param fs the source {@link FileSystem} to base off of
   * @return A {@link FileSystem} with the subject set to the provided user
   */
  public static FileSystem createFileSystemForUser(
      String user, FileSystem fs) {
    if (user == null) {
      // Used to return the top-level FileSystem view when not using Authentication
      return fs;
    }

    final Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    return FileSystem.Factory.get(subject, fs.getConf());
  }

  /**
   * This helper method is used to set the ETag xAttr on an object.
   * @param fs The {@link FileSystem} used to make the gRPC request
   * @param objectUri The {@link AlluxioURI} for the object to update
   * @param entityTag The entity tag of the object (MD5 checksum of the object contents)
   * @throws IOException
   * @throws AlluxioException
   */
  public static void setEntityTag(FileSystem fs, AlluxioURI objectUri, String entityTag)
      throws IOException, AlluxioException {
    fs.setAttribute(objectUri, SetAttributePOptions.newBuilder()
        .putXattr(S3Constants.ETAG_XATTR_KEY,
            ByteString.copyFrom(entityTag, S3Constants.XATTR_STR_CHARSET))
        .setXattrUpdateStrategy(alluxio.proto.journal.File.XAttrUpdateStrategy.UNION_REPLACE)
        .build());
  }

  /**
   * Given a URL-encoded Tagging header, parses and deserializes the Tagging metadata
   * into a {@link TaggingData} object. Returns null on empty strings.
   * @param taggingHeader the URL-encoded Tagging header
   * @param maxHeaderMetadataSize Header user-metadata size limit validation (default max: 2 KB)
   * - https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
   * @return the deserialized {@link TaggingData} object
   */
  public static TaggingData deserializeTaggingHeader(
      String taggingHeader, int maxHeaderMetadataSize) throws S3Exception {
    if (taggingHeader == null || taggingHeader.isEmpty()) { return null; }
    if (maxHeaderMetadataSize > 0
        && taggingHeader.getBytes(S3Constants.TAGGING_CHARSET).length
        > maxHeaderMetadataSize) {
      throw new S3Exception(S3ErrorCode.METADATA_TOO_LARGE);
    }
    Map<String, String> tagMap = new HashMap<>();
    for (String tag : taggingHeader.split("&")) {
      String[] entries = tag.split("=");
      if (entries.length > 1) {
        tagMap.put(entries[0], entries[1]);
      } else { // Key was provided without a value
        tagMap.put(entries[0], "");
      }
    }
    return new TaggingData().addTags(tagMap);
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource object complete path
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toObjectS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (S3Exception e) {
      e.setResource(resource);
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(e, resource, S3ErrorCode.PRECONDITION_FAILED);
    } catch (FileDoesNotExistException e) {
      return new S3Exception(e, resource, S3ErrorCode.NO_SUCH_KEY);
    } catch (AccessControlException e) {
      return new S3Exception(e, resource, S3ErrorCode.ACCESS_DENIED_ERROR);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }
}
