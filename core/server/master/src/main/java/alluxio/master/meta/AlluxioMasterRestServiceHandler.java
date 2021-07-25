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

import static alluxio.metrics.MetricInfo.UFS_OP_PREFIX;
import static alluxio.metrics.MetricInfo.UFS_OP_SAVED_PREFIX;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.StorageTierAssoc;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.meta.MountTable;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.user.ServerUserState;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.LogUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.webui.NodeInfo;
import alluxio.util.webui.StorageTierInfo;
import alluxio.util.webui.UIFileBlockInfo;
import alluxio.util.webui.UIFileInfo;
import alluxio.util.webui.WebUtils;
import alluxio.web.MasterWebServer;
import alluxio.wire.AlluxioMasterInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.Capacity;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MasterWebUIBrowse;
import alluxio.wire.MasterWebUIConfiguration;
import alluxio.wire.MasterWebUIData;
import alluxio.wire.MasterWebUIInit;
import alluxio.wire.MasterWebUILogs;
import alluxio.wire.MasterWebUIMetrics;
import alluxio.wire.MasterWebUIMountTable;
import alluxio.wire.MasterWebUIOverview;
import alluxio.wire.MasterWebUIWorkers;
import alluxio.wire.MountPointInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

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
 * This class is a REST handler for requesting general master information.
 */
@NotThreadSafe
@Api(value = "/master", description = "Alluxio Master Rest Service")
@Path(AlluxioMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioMasterRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterRestServiceHandler.class);

  public static final String SERVICE_PREFIX = "master";

  // endpoints
  public static final String GET_INFO = "info";

  // webui endpoints // TODO(william): DRY up these endpoints
  public static final String WEBUI_INIT = "webui_init";
  public static final String WEBUI_OVERVIEW = "webui_overview";
  public static final String WEBUI_BROWSE = "webui_browse";
  public static final String WEBUI_DATA = "webui_data";
  public static final String WEBUI_LOGS = "webui_logs";
  public static final String WEBUI_CONFIG = "webui_config";
  public static final String WEBUI_WORKERS = "webui_workers";
  public static final String WEBUI_METRICS = "webui_metrics";
  public static final String WEBUI_MOUNTTABLE = "webui_mounttable";

  // queries
  public static final String QUERY_RAW_CONFIGURATION = "raw_configuration";

  // log
  public static final String LOG_LEVEL = "logLevel";
  public static final String LOG_ARGUMENT_NAME = "logName";
  public static final String LOG_ARGUMENT_LEVEL = "level";

  private final AlluxioMasterProcess mMasterProcess;
  private final BlockMaster mBlockMaster;
  private final FileSystemMaster mFileSystemMaster;
  private final MetaMaster mMetaMaster;
  private final FileSystem mFsClient;

  /**
   * Constructs a new {@link AlluxioMasterRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public AlluxioMasterRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mMasterProcess = (AlluxioMasterProcess) context
        .getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY);
    mBlockMaster = mMasterProcess.getMaster(BlockMaster.class);
    mFileSystemMaster = mMasterProcess.getMaster(FileSystemMaster.class);
    mMetaMaster = mMasterProcess.getMaster(MetaMaster.class);
    mFsClient =
        (FileSystem) context.getAttribute(MasterWebServer.ALLUXIO_FILESYSTEM_CLIENT_RESOURCE_KEY);
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
  @ApiOperation(value = "Get general Alluxio Master service information",
      response = alluxio.wire.AlluxioMasterInfo.class)
  public Response getInfo(@QueryParam(QUERY_RAW_CONFIGURATION) final Boolean rawConfiguration) {
    // TODO(jiri): Add a mechanism for retrieving only a subset of the fields.
    return RestUtils.call(() -> {
      boolean rawConfig = false;
      if (rawConfiguration != null) {
        rawConfig = rawConfiguration;
      }
      return new AlluxioMasterInfo().setCapacity(getCapacityInternal())
          .setConfiguration(getConfigurationInternal(rawConfig))
          .setLostWorkers(mBlockMaster.getLostWorkersInfoList()).setMetrics(getMetricsInternal())
          .setMountPoints(getMountPointsInternal())
          .setRpcAddress(mMasterProcess.getRpcAddress().toString())
          .setStartTimeMs(mMasterProcess.getStartTimeMs())
          .setTierCapacity(getTierCapacityInternal()).setUfsCapacity(getUfsCapacityInternal())
          .setUptimeMs(mMasterProcess.getUptimeMs()).setVersion(RuntimeConstants.VERSION)
          .setWorkers(mBlockMaster.getWorkerInfoList());
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
      MasterWebUIInit response = new MasterWebUIInit();

      String proxyHostname = NetworkAddressUtils
          .getConnectHost(NetworkAddressUtils.ServiceType.PROXY_WEB, ServerConfiguration.global());
      int proxyPort = ServerConfiguration.getInt(PropertyKey.PROXY_WEB_PORT);
      Map<String, String> proxyDowloadFileApiUrl = new HashMap<>();
      proxyDowloadFileApiUrl
          .put("prefix", "http://" + proxyHostname + ":" + proxyPort + "/api/v1/paths/");
      proxyDowloadFileApiUrl.put("suffix", "/download-file/");

      response.setDebug(ServerConfiguration.getBoolean(PropertyKey.DEBUG))
          .setNewerVersionAvailable(mMetaMaster.getNewerVersionAvailable())
          .setWebFileInfoEnabled(ServerConfiguration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED))
          .setSecurityAuthorizationPermissionEnabled(
              ServerConfiguration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED))
          .setWorkerPort(ServerConfiguration.getInt(PropertyKey.WORKER_WEB_PORT))
          .setRefreshInterval((int) ServerConfiguration.getMs(PropertyKey.WEB_REFRESH_INTERVAL))
          .setProxyDownloadFileApiUrl(proxyDowloadFileApiUrl);

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets Web UI overview page data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_OVERVIEW)
  public Response getWebUIOverview() {
    return RestUtils.call(() -> {
      MasterWebUIOverview response = new MasterWebUIOverview();

      response.setDebug(ServerConfiguration.getBoolean(PropertyKey.DEBUG))
          .setMasterNodeAddress(mMasterProcess.getRpcAddress().toString()).setUptime(CommonUtils
          .convertMsToClockTime(System.currentTimeMillis() - mMetaMaster.getStartTimeMs()))
          .setStartTime(CommonUtils.convertMsToDate(mMetaMaster.getStartTimeMs(),
              ServerConfiguration.get(PropertyKey.USER_DATE_FORMAT_PATTERN)))
          .setVersion(RuntimeConstants.VERSION)
          .setLiveWorkerNodes(Integer.toString(mBlockMaster.getWorkerCount()))
          .setCapacity(FormatUtils.getSizeFromBytes(mBlockMaster.getCapacityBytes()))
          .setClusterId(mMetaMaster.getClusterID())
          .setUsedCapacity(FormatUtils.getSizeFromBytes(mBlockMaster.getUsedBytes()))
          .setFreeCapacity(FormatUtils
              .getSizeFromBytes(mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes()));
      ConfigCheckReport report = mMetaMaster.getConfigCheckReport();
      response.setConfigCheckStatus(report.getConfigStatus())
          .setConfigCheckErrors(report.getConfigErrors())
          .setConfigCheckWarns(report.getConfigWarns()).setConfigCheckErrorNum(
          report.getConfigErrors().values().stream().mapToInt(List::size).sum())
          .setConfigCheckWarnNum(
              report.getConfigWarns().values().stream().mapToInt(List::size).sum());

      StorageTierAssoc globalStorageTierAssoc = mBlockMaster.getGlobalStorageTierAssoc();
      List<StorageTierInfo> infosList = new ArrayList<>();
      Map<String, Long> totalBytesOnTiers = mBlockMaster.getTotalBytesOnTiers();
      Map<String, Long> usedBytesOnTiers = mBlockMaster.getUsedBytesOnTiers();

      for (int ordinal = 0; ordinal < globalStorageTierAssoc.size(); ordinal++) {
        String tierAlias = globalStorageTierAssoc.getAlias(ordinal);
        if (totalBytesOnTiers.containsKey(tierAlias) && totalBytesOnTiers.get(tierAlias) > 0) {
          StorageTierInfo info = new StorageTierInfo(tierAlias, totalBytesOnTiers.get(tierAlias),
              usedBytesOnTiers.get(tierAlias));
          infosList.add(info);
        }
      }

      response.setStorageTierInfos(infosList);

      MountPointInfo mountInfo;
      try {
        mountInfo = mFileSystemMaster.getDisplayMountPointInfo(new AlluxioURI(MountTable.ROOT));

        long capacityBytes = mountInfo.getUfsCapacityBytes();
        long usedBytes = mountInfo.getUfsUsedBytes();
        long freeBytes = -1;
        if (usedBytes >= 0 && capacityBytes >= usedBytes) {
          freeBytes = capacityBytes - usedBytes;
        }

        String totalSpace = "UNKNOWN";
        if (capacityBytes >= 0) {
          totalSpace = FormatUtils.getSizeFromBytes(capacityBytes);
        }
        response.setDiskCapacity(totalSpace);

        String usedSpace = "UNKNOWN";
        if (usedBytes >= 0) {
          usedSpace = FormatUtils.getSizeFromBytes(usedBytes);
        }
        response.setDiskUsedCapacity(usedSpace);

        String freeSpace = "UNKNOWN";
        if (freeBytes >= 0) {
          freeSpace = FormatUtils.getSizeFromBytes(freeBytes);
        }
        response.setDiskFreeCapacity(freeSpace);
      } catch (Throwable e) {
        response.setDiskCapacity("UNKNOWN").setDiskUsedCapacity("UNKNOWN")
            .setDiskFreeCapacity("UNKNOWN");
      }
      mMetaMaster.getJournalSpaceMonitor().map(monitor ->
          response.setJournalDiskWarnings(monitor.getJournalDiskWarnings()));

      Gauge entriesSinceGauge = MetricsSystem.METRIC_REGISTRY.getGauges()
              .get(MetricKey.MASTER_JOURNAL_ENTRIES_SINCE_CHECKPOINT.getName());
      Gauge lastCkPtGauge = MetricsSystem.METRIC_REGISTRY.getGauges()
          .get(MetricKey.MASTER_JOURNAL_LAST_CHECKPOINT_TIME.getName());

      if (entriesSinceGauge != null && lastCkPtGauge != null) {
        long entriesSinceCkpt = (Long) entriesSinceGauge.getValue();
        long lastCkptTime = (Long) lastCkPtGauge.getValue();
        long timeSinceCkpt = System.currentTimeMillis() - lastCkptTime;
        boolean overThreshold = timeSinceCkpt > ServerConfiguration.getMs(
            PropertyKey.MASTER_WEB_JOURNAL_CHECKPOINT_WARNING_THRESHOLD_TIME);
        boolean passedThreshold = entriesSinceCkpt > ServerConfiguration
            .getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);
        if (passedThreshold && overThreshold) {
          String time = lastCkptTime > 0 ? ZonedDateTime
              .ofInstant(Instant.ofEpochMilli(lastCkptTime), ZoneOffset.UTC)
              .format(DateTimeFormatter.ISO_INSTANT) : "N/A";
          String advice = ConfigurationUtils.isHaMode(ServerConfiguration.global()) ? ""
              : "It is recommended to use the fsadmin tool to checkpoint the journal. This will "
              + "prevent the master from serving requests while checkpointing.";
          response.setJournalCheckpointTimeWarning(String.format("Journal has not checkpointed in "
              + "a timely manner since passing the checkpoint threshold (%d/%d). Last checkpoint:"
              + " %s. %s",
              entriesSinceCkpt,
              ServerConfiguration.getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES),
              time, advice));
        }
      }

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets Web UI browse page data.
   *
   * @param requestPath the request path
   * @param requestOffset the request offset
   * @param requestEnd the request end
   * @param requestLimit the request limit
   * @return the response object
   */
  @GET
  @Path(WEBUI_BROWSE)
  public Response getWebUIBrowse(@DefaultValue("/") @QueryParam("path") String requestPath,
      @DefaultValue("0") @QueryParam("offset") String requestOffset,
      @DefaultValue("") @QueryParam("end") String requestEnd,
      @DefaultValue("20") @QueryParam("limit") String requestLimit) {
    return RestUtils.call(() -> {
      MasterWebUIBrowse response = new MasterWebUIBrowse();

      if (!ServerConfiguration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return response;
      }

      if (SecurityUtils.isSecurityEnabled(ServerConfiguration.global())
          && AuthenticatedClientUser.get(ServerConfiguration.global()) == null) {
        AuthenticatedClientUser.set(ServerUserState.global().getUser().getName());
      }
      response.setDebug(ServerConfiguration.getBoolean(PropertyKey.DEBUG)).setShowPermissions(
          ServerConfiguration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED))
          .setMasterNodeAddress(mMasterProcess.getRpcAddress().toString()).setInvalidPathError("");
      List<FileInfo> filesInfo;
      String path = URLDecoder.decode(requestPath, "UTF-8");
      if (path.isEmpty()) {
        path = AlluxioURI.SEPARATOR;
      }
      AlluxioURI currentPath = new AlluxioURI(path);
      response.setCurrentPath(currentPath.toString()).setViewingOffset(0);

      try {
        long fileId = mFileSystemMaster.getFileId(currentPath);
        FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
        UIFileInfo currentFileInfo = new UIFileInfo(fileInfo, ServerConfiguration.global(),
            new MasterStorageTierAssoc().getOrderedStorageAliases());
        if (currentFileInfo.getAbsolutePath() == null) {
          throw new FileDoesNotExistException(currentPath.toString());
        }
        response.setCurrentDirectory(currentFileInfo)
            .setBlockSizeBytes(currentFileInfo.getBlockSizeBytes());
        if (!currentFileInfo.getIsDirectory()) {
          long relativeOffset = 0;
          long offset;
          try {
            if (requestOffset != null) {
              relativeOffset = Long.parseLong(requestOffset);
            }
          } catch (NumberFormatException e) {
            // ignore the exception
          }
          // If no param "end" presents, the offset is relative to the beginning; otherwise, it is
          // relative to the end of the file.
          if (requestEnd.equals("")) {
            offset = relativeOffset;
          } else {
            offset = fileInfo.getLength() - relativeOffset;
          }
          if (offset < 0) {
            offset = 0;
          } else if (offset > fileInfo.getLength()) {
            offset = fileInfo.getLength();
          }
          try {
            AlluxioURI absolutePath = new AlluxioURI(currentFileInfo.getAbsolutePath());
            FileSystem fs = mFsClient;
            String fileData;
            URIStatus status = fs.getStatus(absolutePath);
            if (status.isCompleted()) {
              OpenFilePOptions options =
                  OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build();
              try (FileInStream is = fs.openFile(absolutePath, options)) {
                int len = (int) Math.min(5L * Constants.KB, status.getLength() - offset);
                byte[] data = new byte[len];
                long skipped = is.skip(offset);
                if (skipped < 0) {
                  // nothing was skipped
                  fileData = "Unable to traverse to offset; is file empty?";
                } else if (skipped < offset) {
                  // couldn't skip all the way to offset
                  fileData = "Unable to traverse to offset; is offset larger than the file?";
                } else {
                  // read may not read up to len, so only convert what was read
                  int read = is.read(data, 0, len);
                  if (read < 0) {
                    // stream couldn't read anything, skip went to EOF?
                    fileData = "Unable to read file";
                  } else {
                    fileData = WebUtils.convertByteArrayToStringWithoutEscape(data, 0, read);
                  }
                }
              }
            } else {
              fileData = "The requested file is not complete yet.";
            }
            List<UIFileBlockInfo> uiBlockInfo = new ArrayList<>();
            for (FileBlockInfo fileBlockInfo : mFileSystemMaster
                .getFileBlockInfoList(absolutePath)) {
              uiBlockInfo.add(new UIFileBlockInfo(fileBlockInfo, ServerConfiguration.global()));
            }
            response.setFileBlocks(uiBlockInfo).setFileData(fileData)
                .setHighestTierAlias(mBlockMaster.getGlobalStorageTierAssoc().getAlias(0));
          } catch (AlluxioException e) {
            throw new IOException(e);
          }
          response.setViewingOffset(offset);
          return response;
        }

        if (currentPath.isRoot()) {
          response.setPathInfos(new UIFileInfo[0]);
        } else {
          String[] splitPath = PathUtils.getPathComponents(currentPath.toString());
          UIFileInfo[] pathInfos = new UIFileInfo[splitPath.length - 1];
          fileId = mFileSystemMaster.getFileId(currentPath);
          pathInfos[0] =
              new UIFileInfo(mFileSystemMaster.getFileInfo(fileId), ServerConfiguration.global(),
                  new MasterStorageTierAssoc().getOrderedStorageAliases());
          AlluxioURI breadcrumb = new AlluxioURI(AlluxioURI.SEPARATOR);
          for (int i = 1; i < splitPath.length - 1; i++) {
            breadcrumb = breadcrumb.join(splitPath[i]);
            fileId = mFileSystemMaster.getFileId(breadcrumb);
            pathInfos[i] =
                new UIFileInfo(mFileSystemMaster.getFileInfo(fileId), ServerConfiguration.global(),
                    new MasterStorageTierAssoc().getOrderedStorageAliases());
          }
          response.setPathInfos(pathInfos);
        }

        filesInfo = mFileSystemMaster.listStatus(currentPath, ListStatusContext.defaults());
      } catch (FileDoesNotExistException e) {
        response.setInvalidPathError("Error: Invalid Path " + e.getMessage());
        return response;
      } catch (InvalidPathException e) {
        response.setInvalidPathError("Error: Invalid Path " + e.getLocalizedMessage());
        return response;
      } catch (UnavailableException e) {
        response.setInvalidPathError("The service is temporarily unavailable. " + e.getMessage());
        return response;
      } catch (IOException e) {
        response.setInvalidPathError(
            "Error: File " + currentPath + " is not available " + e.getMessage());
        return response;
      } catch (AccessControlException e) {
        response.setInvalidPathError(
            "Error: File " + currentPath + " cannot be accessed " + e.getMessage());
        return response;
      }

      List<UIFileInfo> fileInfos = new ArrayList<>(filesInfo.size());
      for (FileInfo fileInfo : filesInfo) {
        UIFileInfo toAdd = new UIFileInfo(fileInfo, ServerConfiguration.global(),
            new MasterStorageTierAssoc().getOrderedStorageAliases());
        try {
          if (!toAdd.getIsDirectory() && fileInfo.getLength() > 0) {
            FileBlockInfo blockInfo =
                mFileSystemMaster.getFileBlockInfoList(new AlluxioURI(toAdd.getAbsolutePath()))
                    .get(0);
            List<String> locations = new ArrayList<>();
            // add the in-Alluxio block locations
            for (BlockLocation location : blockInfo.getBlockInfo().getLocations()) {
              WorkerNetAddress address = location.getWorkerAddress();
              locations.add(address.getHost() + ":" + address.getDataPort());
            }
            // add underFS locations
            locations.addAll(blockInfo.getUfsLocations());
            toAdd.setFileLocations(locations);
          }
        } catch (FileDoesNotExistException e) {
          response.setFileDoesNotExistException("Error: non-existing file " + e.getMessage());
          return response;
        } catch (InvalidPathException e) {
          response.setInvalidPathException("Error: invalid path " + e.getMessage());
          return response;
        } catch (AccessControlException e) {
          response.setAccessControlException(
              "Error: File " + currentPath + " cannot be accessed " + e.getMessage());
          return response;
        }
        fileInfos.add(toAdd);
      }
      fileInfos.sort(UIFileInfo.PATH_STRING_COMPARE);

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

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets Web UI data page data.
   *
   * @param requestOffset the request offset
   * @param requestLimit the request limit
   * @return the response object
   */
  @GET
  @Path(WEBUI_DATA)
  public Response getWebUIData(@DefaultValue("0") @QueryParam("offset") String requestOffset,
      @DefaultValue("20") @QueryParam("limit") String requestLimit) {
    return RestUtils.call(() -> {
      MasterWebUIData response = new MasterWebUIData();

      if (!ServerConfiguration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return response;
      }

      if (SecurityUtils.isSecurityEnabled(ServerConfiguration.global())
          && AuthenticatedClientUser.get(ServerConfiguration.global()) == null) {
        AuthenticatedClientUser.set(ServerUserState.global().getUser().getName());
      }
      response.setMasterNodeAddress(mMasterProcess.getRpcAddress().toString()).setFatalError("")
          .setShowPermissions(ServerConfiguration
              .getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED));

      List<AlluxioURI> inAlluxioFiles = mFileSystemMaster.getInAlluxioFiles();
      Collections.sort(inAlluxioFiles);

      List<UIFileInfo> fileInfos = new ArrayList<>(inAlluxioFiles.size());
      for (AlluxioURI file : inAlluxioFiles) {
        try {
          long fileId = mFileSystemMaster.getFileId(file);
          FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
          if (fileInfo != null && fileInfo.getInAlluxioPercentage() == 100) {
            fileInfos.add(new UIFileInfo(fileInfo, ServerConfiguration.global(),
                new MasterStorageTierAssoc().getOrderedStorageAliases()));
          }
        } catch (FileDoesNotExistException e) {
          response.setFatalError("Error: File does not exist " + e.getLocalizedMessage());
          return response;
        } catch (AccessControlException e) {
          response
              .setPermissionError("Error: File " + file + " cannot be accessed " + e.getMessage());
          return response;
        }
      }
      response.setInAlluxioFileNum(fileInfos.size());

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

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets Web UI logs page data.
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
      @DefaultValue("") @QueryParam("end") String requestEnd,
      @DefaultValue("20") @QueryParam("limit") String requestLimit) {
    return RestUtils.call(() -> {
      FilenameFilter filenameFilter = (dir, name) -> name.toLowerCase().endsWith(".log");
      MasterWebUILogs response = new MasterWebUILogs();

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
                new MasterStorageTierAssoc().getOrderedStorageAliases()));
          }
        }
        fileInfos.sort(UIFileInfo.PATH_STRING_COMPARE);
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
            // ignore the exception
          }
          // If no param "end" presents, the offset is relative to the beginning; otherwise, it is
          // relative to the end of the file.
          if (requestEnd.equals("")) {
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

  /**
   * Gets Web UI ServerConfiguration page data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_CONFIG)
  public Response getWebUIConfiguration() {
    return RestUtils.call(() -> {
      MasterWebUIConfiguration response = new MasterWebUIConfiguration();

      response.setWhitelist(mFileSystemMaster.getWhiteList());

      TreeSet<Triple<String, String, String>> sortedProperties = new TreeSet<>();
      Set<String> alluxioConfExcludes = Sets.newHashSet(PropertyKey.MASTER_WHITELIST.toString());
      for (ConfigProperty configProperty : mMetaMaster
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
    }, ServerConfiguration.global());
  }

  /**
   * Gets Web UI workers page data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_WORKERS)
  public Response getWebUIWorkers() {
    return RestUtils.call(() -> {
      MasterWebUIWorkers response = new MasterWebUIWorkers();

      response.setDebug(ServerConfiguration.getBoolean(PropertyKey.DEBUG));

      List<WorkerInfo> workerInfos = mBlockMaster.getWorkerInfoList();
      NodeInfo[] normalNodeInfos = WebUtils.generateOrderedNodeInfos(workerInfos);
      response.setNormalNodeInfos(normalNodeInfos);

      List<WorkerInfo> lostWorkerInfos = mBlockMaster.getLostWorkersInfoList();
      NodeInfo[] failedNodeInfos = WebUtils.generateOrderedNodeInfos(lostWorkerInfos);
      response.setFailedNodeInfos(failedNodeInfos);

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * Gets Web UI mount table page data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_MOUNTTABLE)
  public Response getWebUIMountTable() {
    return RestUtils.call(() -> {
      MasterWebUIMountTable response = new MasterWebUIMountTable();

      response.setDebug(ServerConfiguration.getBoolean(PropertyKey.DEBUG));
      Map<String, MountPointInfo> mountPointInfo = getMountPointsInternal();

      response.setMountPointInfos(mountPointInfo);

      return response;
    }, ServerConfiguration.global());
  }

  /**
   * @param ufs the ufs uri encoded by {@link MetricsSystem#escape(AlluxioURI)}
   * @return whether the ufs uri is a mount point
   */
  @VisibleForTesting
  boolean isMounted(String ufs) {
    ufs = PathUtils.normalizePath(ufs, AlluxioURI.SEPARATOR);
    for (Map.Entry<String, MountPointInfo> entry :
        mFileSystemMaster.getMountPointInfoSummary().entrySet()) {
      String escaped = MetricsSystem.escape(new AlluxioURI(entry.getValue().getUfsUri()));
      escaped = PathUtils.normalizePath(escaped, AlluxioURI.SEPARATOR);
      if (escaped.equals(ufs)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets Web UI metrics page data.
   *
   * @return the response object
   */
  @GET
  @Path(WEBUI_METRICS)
  public Response getWebUIMetrics() {
    return RestUtils.call(() -> {
      MasterWebUIMetrics response = new MasterWebUIMetrics();

      MetricRegistry mr = MetricsSystem.METRIC_REGISTRY;
      SortedMap<String, Gauge> gauges = mr.getGauges();
      SortedMap<String, Counter> counters = mr.getCounters();

      Long masterCapacityTotal = (Long) gauges
          .get(MetricKey.CLUSTER_CAPACITY_TOTAL.getName()).getValue();
      Long masterCapacityUsed = (Long) gauges
          .get(MetricKey.CLUSTER_CAPACITY_USED.getName()).getValue();

      int masterCapacityUsedPercentage =
          (masterCapacityTotal > 0) ? (int) (100L * masterCapacityUsed / masterCapacityTotal) : 0;
      response.setMasterCapacityUsedPercentage(masterCapacityUsedPercentage)
          .setMasterCapacityFreePercentage(100 - masterCapacityUsedPercentage);

      Long masterUnderfsCapacityTotal = (Long) gauges
          .get(MetricKey.CLUSTER_ROOT_UFS_CAPACITY_TOTAL.getName()).getValue();
      Long masterUnderfsCapacityUsed = (Long) gauges
          .get(MetricKey.CLUSTER_ROOT_UFS_CAPACITY_USED.getName()).getValue();

      int masterUnderfsCapacityUsedPercentage =
          (masterUnderfsCapacityTotal > 0) ? (int) (100L * masterUnderfsCapacityUsed
              / masterUnderfsCapacityTotal) : 0;
      response.setMasterUnderfsCapacityUsedPercentage(masterUnderfsCapacityUsedPercentage)
          .setMasterUnderfsCapacityFreePercentage(100 - masterUnderfsCapacityUsedPercentage);

      // cluster read size
      Long bytesReadLocal = counters.get(
          MetricKey.CLUSTER_BYTES_READ_LOCAL.getName()).getCount();
      Long bytesReadRemote = counters.get(
          MetricKey.CLUSTER_BYTES_READ_REMOTE.getName()).getCount();
      Long bytesReadDomainSocket = counters.get(
          MetricKey.CLUSTER_BYTES_READ_DOMAIN.getName()).getCount();
      Long bytesReadUfs = counters.get(
          MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName()).getCount();
      response.setTotalBytesReadLocal(FormatUtils.getSizeFromBytes(bytesReadLocal))
          .setTotalBytesReadDomainSocket(FormatUtils.getSizeFromBytes(bytesReadDomainSocket))
          .setTotalBytesReadRemote(FormatUtils.getSizeFromBytes(bytesReadRemote))
          .setTotalBytesReadUfs(FormatUtils.getSizeFromBytes(bytesReadUfs));

      // cluster cache hit and miss
      long bytesReadTotal = bytesReadLocal + bytesReadRemote + bytesReadDomainSocket;
      double cacheHitLocalPercentage =
          (bytesReadTotal > 0)
              ? (100D * (bytesReadLocal + bytesReadDomainSocket) / bytesReadTotal) : 0;
      double cacheHitRemotePercentage =
          (bytesReadTotal > 0) ? (100D * (bytesReadRemote - bytesReadUfs) / bytesReadTotal) : 0;
      double cacheMissPercentage =
          (bytesReadTotal > 0) ? (100D * bytesReadUfs / bytesReadTotal) : 0;
      response.setCacheHitLocal(String.format("%.2f", cacheHitLocalPercentage))
          .setCacheHitRemote(String.format("%.2f", cacheHitRemotePercentage))
          .setCacheMiss(String.format("%.2f", cacheMissPercentage));

      // cluster write size
      Long bytesWrittenLocal = counters
          .get(MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL.getName()).getCount();
      Long bytesWrittenAlluxio = counters
          .get(MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE.getName()).getCount();
      Long bytesWrittenDomainSocket = counters.get(
          MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN.getName()).getCount();
      Long bytesWrittenUfs = counters
          .get(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName()).getCount();
      response.setTotalBytesWrittenLocal(FormatUtils.getSizeFromBytes(bytesWrittenLocal))
          .setTotalBytesWrittenRemote(FormatUtils.getSizeFromBytes(bytesWrittenAlluxio))
          .setTotalBytesWrittenDomainSocket(FormatUtils.getSizeFromBytes(bytesWrittenDomainSocket))
          .setTotalBytesWrittenUfs(FormatUtils.getSizeFromBytes(bytesWrittenUfs));

      // cluster read throughput
      Long bytesReadLocalThroughput = (Long) gauges.get(
          MetricKey.CLUSTER_BYTES_READ_LOCAL_THROUGHPUT.getName()).getValue();
      Long bytesReadDomainSocketThroughput = (Long) gauges
          .get(MetricKey.CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT.getName()).getValue();
      Long bytesReadRemoteThroughput = (Long) gauges
          .get(MetricKey.CLUSTER_BYTES_READ_REMOTE_THROUGHPUT.getName()).getValue();
      Long bytesReadUfsThroughput = (Long) gauges
          .get(MetricKey.CLUSTER_BYTES_READ_UFS_THROUGHPUT.getName()).getValue();
      response
          .setTotalBytesReadLocalThroughput(FormatUtils.getSizeFromBytes(bytesReadLocalThroughput))
          .setTotalBytesReadDomainSocketThroughput(
              FormatUtils.getSizeFromBytes(bytesReadDomainSocketThroughput))
          .setTotalBytesReadRemoteThroughput(
              FormatUtils.getSizeFromBytes(bytesReadRemoteThroughput))
          .setTotalBytesReadUfsThroughput(FormatUtils.getSizeFromBytes(bytesReadUfsThroughput));

      // cluster write throughput
      Long bytesWrittenLocalThroughput = (Long) gauges
          .get(MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL_THROUGHPUT.getName())
          .getValue();
      Long bytesWrittenAlluxioThroughput = (Long) gauges
          .get(MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE_THROUGHPUT.getName()).getValue();
      Long bytesWrittenDomainSocketThroughput = (Long) gauges.get(
          MetricKey.CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT.getName()).getValue();
      Long bytesWrittenUfsThroughput = (Long) gauges
          .get(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT.getName()).getValue();
      response.setTotalBytesWrittenLocalThroughput(
              FormatUtils.getSizeFromBytes(bytesWrittenLocalThroughput))
          .setTotalBytesWrittenRemoteThroughput(
              FormatUtils.getSizeFromBytes(bytesWrittenAlluxioThroughput))
          .setTotalBytesWrittenDomainSocketThroughput(
              FormatUtils.getSizeFromBytes(bytesWrittenDomainSocketThroughput))
          .setTotalBytesWrittenUfsThroughput(
              FormatUtils.getSizeFromBytes(bytesWrittenUfsThroughput));

      //
      // For the per UFS metrics below, if a UFS has been unmounted, its metrics will still exist
      // in the metrics system, but we don't show them in the UI. After remounting the UFS, the
      // new metrics will be accumulated on its old values and shown in the UI.
      //

      // cluster per UFS read
      Map<String, String> ufsReadSizeMap = new TreeMap<>();
      Map<String, String> ufsWriteSizeMap = new TreeMap<>();
      Map<String, Counter> rpcInvocations = new TreeMap<>();
      Map<String, Metric> operations = new TreeMap<>();
      // UFS : (OPS : Count)
      Map<String, Map<String, Long>> ufsOpsSavedMap = new TreeMap<>();

      for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        String metricName = entry.getKey();
        long value = entry.getValue().getCount();
        if (metricName.contains(MetricKey.CLUSTER_BYTES_READ_UFS.getName())) {
          String ufs = alluxio.metrics.Metric.getTagUfsValueFromFullName(metricName);
          if (ufs != null && isMounted(ufs)) {
            ufsReadSizeMap.put(ufs, FormatUtils.getSizeFromBytes(value));
          }
        } else if (metricName.contains(MetricKey.CLUSTER_BYTES_WRITTEN_UFS.getName())) {
          String ufs = alluxio.metrics.Metric.getTagUfsValueFromFullName(metricName);
          if (ufs != null && isMounted(ufs)) {
            ufsWriteSizeMap.put(ufs, FormatUtils.getSizeFromBytes(value));
          }
        } else if (metricName.endsWith("Ops")) {
          rpcInvocations
              .put(MetricsSystem.stripInstanceAndHost(metricName), entry.getValue());
        } else if (metricName.contains(UFS_OP_SAVED_PREFIX)) {
          String ufs = alluxio.metrics.Metric.getTagUfsValueFromFullName(metricName);
          if (ufs != null && isMounted(ufs)) {
            // Unescape the URI for display
            String ufsUnescaped = MetricsSystem.unescape(ufs);
            Map<String, Long> perUfsMap = ufsOpsSavedMap.getOrDefault(
                ufsUnescaped, new TreeMap<>());
            String alluxioOperation = alluxio.metrics.Metric.getBaseName(metricName)
                .substring(UFS_OP_SAVED_PREFIX.length());
            String equivalentOp = DefaultFileSystemMaster.Metrics.UFS_OPS_DESC.get(
                DefaultFileSystemMaster.Metrics.UFSOps.valueOf(alluxioOperation));
            if (equivalentOp != null) {
              alluxioOperation = String.format("%s (Roughly equivalent to %s operation)",
                  alluxioOperation, equivalentOp);
            }
            perUfsMap.put(alluxioOperation, entry.getValue().getCount());
            ufsOpsSavedMap.put(ufsUnescaped, perUfsMap);
          }
        } else {
          operations
              .put(MetricsSystem.stripInstanceAndHost(metricName), entry.getValue());
        }
      }

      String filesPinnedProperty = MetricKey.MASTER_FILES_PINNED.getName();
      operations.put(MetricsSystem.stripInstanceAndHost(filesPinnedProperty),
          gauges.get(filesPinnedProperty));

      response.setOperationMetrics(operations).setRpcInvocationMetrics(rpcInvocations);

      response.setUfsReadSize(ufsReadSizeMap);
      response.setUfsWriteSize(ufsWriteSizeMap);
      response.setUfsOpsSaved(ufsOpsSavedMap);

      // per UFS ops
      Map<String, Map<String, Long>> ufsOpsMap = new TreeMap<>();
      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        String metricName = entry.getKey();
        if (metricName.contains(UFS_OP_PREFIX)) {
          String ufs = alluxio.metrics.Metric.getTagUfsValueFromFullName(metricName);
          if (ufs != null && isMounted(ufs)) {
            // Unescape the URI for display
            String ufsUnescaped = MetricsSystem.unescape(ufs);
            Map<String, Long> perUfsMap = ufsOpsMap.getOrDefault(ufsUnescaped, new TreeMap<>());
            perUfsMap.put(alluxio.metrics.Metric.getBaseName(metricName)
                .substring(UFS_OP_PREFIX.length()), (Long) entry.getValue().getValue());
            ufsOpsMap.put(ufsUnescaped, perUfsMap);
          }
        }
      }
      response.setUfsOps(ufsOpsMap);

      response.setTimeSeriesMetrics(mFileSystemMaster.getTimeSeries());
      mMetaMaster.getJournalSpaceMonitor().ifPresent(monitor -> {
        try {
          response.setJournalDiskMetrics(monitor.getDiskInfo());
        } catch (IOException e) {
          LogUtils.warnWithException(LOG,
              "Failed to populate journal disk information for WebUI metrics.", e);
        }
      });
      if (response.getJournalDiskMetrics() == null) {
        response.setJournalDiskMetrics(Collections.emptyList());
      }

      Gauge lastCheckpointTimeGauge =
          gauges.get(MetricKey.MASTER_JOURNAL_LAST_CHECKPOINT_TIME.getName());
      Gauge entriesSinceCheckpointGauge =
          gauges.get(MetricKey.MASTER_JOURNAL_ENTRIES_SINCE_CHECKPOINT.getName());

      if (entriesSinceCheckpointGauge != null) {
        response.setJournalEntriesSinceCheckpoint((long) entriesSinceCheckpointGauge.getValue());
      }
      if (lastCheckpointTimeGauge != null) {
        long lastCheckpointTime = (long) lastCheckpointTimeGauge.getValue();
        String time;
        if (lastCheckpointTime > 0) {
          time = ZonedDateTime
              .ofInstant(Instant.ofEpochMilli(lastCheckpointTime), ZoneOffset.UTC)
              .format(DateTimeFormatter.ISO_INSTANT);
        } else {
          time = "N/A";
        }
        response.setJournalLastCheckpointTime(time);
      }

      return response;
    }, ServerConfiguration.global());
  }

  private Comparator<String> getTierAliasComparator() {
    return new Comparator<String>() {
      private MasterStorageTierAssoc mTierAssoc = new MasterStorageTierAssoc();

      @Override
      public int compare(String tier1, String tier2) {
        int ordinal1 = mTierAssoc.getOrdinal(tier1);
        int ordinal2 = mTierAssoc.getOrdinal(tier2);
        return Integer.compare(ordinal1, ordinal2);
      }
    };
  }

  private Capacity getCapacityInternal() {
    return new Capacity().setTotal(mBlockMaster.getCapacityBytes())
        .setUsed(mBlockMaster.getUsedBytes());
  }

  private Map<String, String> getConfigurationInternal(boolean raw) {
    return new TreeMap<>(ServerConfiguration
        .toMap(ConfigurationValueOptions.defaults().useDisplayValue(true).useRawValue(raw)));
  }

  private Map<String, Long> getMetricsInternal() {
    MetricRegistry metricRegistry = MetricsSystem.METRIC_REGISTRY;

    // Get all counters.
    Map<String, Counter> counters = metricRegistry.getCounters();
    // Only the gauge for pinned files is retrieved here, other gauges are statistics of
    // free/used
    // spaces, those statistics can be gotten via other REST apis.
    String filesPinnedProperty = MetricKey.MASTER_FILES_PINNED.getName();
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
    return mFileSystemMaster.getMountPointInfoSummary();
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

  private Capacity getUfsCapacityInternal() {
    MountPointInfo mountInfo = mFileSystemMaster.getMountPointInfoSummary().get(MountTable.ROOT);
    if (mountInfo == null) {
      return new Capacity().setTotal(-1).setUsed(-1);
    }
    long capacityBytes = mountInfo.getUfsCapacityBytes();
    long usedBytes = mountInfo.getUfsUsedBytes();
    return new Capacity().setTotal(capacityBytes).setUsed(usedBytes);
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
