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

import static alluxio.Configuration.getBoolean;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationValueOptions;
import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.PropertyKey;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.StorageTierAssoc;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterProcess;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.StartupConsistencyCheck;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MasterMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.WorkerMetrics;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.LogUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.io.PathUtils;
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
import alluxio.wire.ConfigProperty;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.GetConfigurationOptions;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.MasterWebUILogs;
import alluxio.wire.MasterWebUIMetrics;
import alluxio.wire.MountPointInfo;
import alluxio.wire.MasterWebUIBrowse;
import alluxio.wire.MasterWebUIConfiguration;
import alluxio.wire.MasterWebUIData;
import alluxio.wire.MasterWebUIOverview;
import alluxio.wire.MasterWebUIWorkers;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.qmino.miredot.annotations.ReturnType;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
@Path(AlluxioMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioMasterRestServiceHandler {

  public static final String SERVICE_PREFIX = "master";

  // endpoints
  public static final String GET_INFO = "info";
  public static final String WEBUI_OVERVIEW = "webui_overview";
  public static final String WEBUI_BROWSE = "webui_browse";
  public static final String WEBUI_DATA = "webui_data";
  public static final String WEBUI_LOGS = "webui_logs";
  public static final String WEBUI_CONFIG = "webui_config";
  public static final String WEBUI_WORKERS = "webui_workers";
  public static final String WEBUI_METRICS = "webui_metrics";

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
  private final MetaMaster mMetaMaster;

  /**
   * Constructs a new {@link AlluxioMasterRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public AlluxioMasterRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mMasterProcess =
        (MasterProcess) context.getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY);
    mBlockMaster = mMasterProcess.getMaster(BlockMaster.class);
    mFileSystemMaster = mMasterProcess.getMaster(FileSystemMaster.class);
    mMetaMaster = mMasterProcess.getMaster(MetaMaster.class);
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
          .setStartupConsistencyCheck(getStartupConsistencyCheckInternal())
          .setTierCapacity(getTierCapacityInternal()).setUfsCapacity(getUfsCapacityInternal())
          .setUptimeMs(mMasterProcess.getUptimeMs()).setVersion(RuntimeConstants.VERSION)
          .setWorkers(mBlockMaster.getWorkerInfoList());
    });
  }

  /**
   * @summary get the information rendered in the Web UI's overview page
   * @return the response object
   */
  @GET
  @Path(WEBUI_OVERVIEW)
  @ReturnType("alluxio.wire.MasterWebUIOverview")
  public Response getWebUIOverview() {
    return RestUtils.call(() -> {
      alluxio.wire.StartupConsistencyCheck startupConsistencyCheck =
          getStartupConsistencyCheckInternal();

      ConfigCheckReport report = mMetaMaster.getConfigCheckReport();

      MountPointInfo mountInfo = null;
      String diskCapacity;
      String diskUsedCapacity;
      String diskFreeCapacity;
      try {
        mountInfo = mFileSystemMaster.getMountPointInfo(new AlluxioURI(MountTable.ROOT));
      } catch (Throwable e) {
        diskCapacity = "UNKNOWN";
        diskUsedCapacity = "UNKNOWN";
        diskFreeCapacity = "UNKNOWN";
      }
      long capacityBytes = mountInfo.getUfsCapacityBytes();
      long usedBytes = mountInfo.getUfsUsedBytes();
      long freeBytes = -1;
      if (capacityBytes >= 0 && usedBytes >= 0 && capacityBytes >= usedBytes) {
        freeBytes = capacityBytes - usedBytes;
      }

      String totalSpace = "UNKNOWN";
      if (capacityBytes >= 0) {
        totalSpace = FormatUtils.getSizeFromBytes(capacityBytes);
      }
      diskCapacity = totalSpace;

      String usedSpace = "UNKNOWN";
      if (usedBytes >= 0) {
        usedSpace = FormatUtils.getSizeFromBytes(usedBytes);
      }
      diskUsedCapacity = usedSpace;

      String freeSpace = "UNKNOWN";
      if (freeBytes >= 0) {
        freeSpace = FormatUtils.getSizeFromBytes(freeBytes);
      }
      diskFreeCapacity = freeSpace;

      StorageTierAssoc globalStorageTierAssoc = mBlockMaster.getGlobalStorageTierAssoc();
      List<StorageTierInfo> infos = new ArrayList<>();
      Map<String, Long> totalBytesOnTiers = mBlockMaster.getTotalBytesOnTiers();
      Map<String, Long> usedBytesOnTiers = mBlockMaster.getUsedBytesOnTiers();

      for (int ordinal = 0; ordinal < globalStorageTierAssoc.size(); ordinal++) {
        String tierAlias = globalStorageTierAssoc.getAlias(ordinal);
        if (totalBytesOnTiers.containsKey(tierAlias) && totalBytesOnTiers.get(tierAlias) > 0) {
          StorageTierInfo info = new StorageTierInfo(tierAlias, totalBytesOnTiers.get(tierAlias),
              usedBytesOnTiers.get(tierAlias));
          infos.add(info);
        }
      }

      return new MasterWebUIOverview().setCapacity(getCapacityInternal()).setConfigCheckErrorNum(
          report.getConfigErrors().values().stream().mapToInt(List::size).sum())
          .setConfigCheckErrors(report.getConfigErrors())
          .setConfigCheckStatus(report.getConfigStatus())
          .setConfigCheckWarns(report.getConfigWarns())
          .setConsistencyCheckStatus(startupConsistencyCheck.getStatus())
          .setDebug(getBoolean(PropertyKey.DEBUG)).setDiskCapacity(diskCapacity)
          .setDiskCapacity(totalSpace).setDiskFreeCapacity(diskFreeCapacity)
          .setDiskUsedCapacity(diskUsedCapacity).setFreeCapacity(FormatUtils
              .getSizeFromBytes(mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes()))
          .setInconsistentPathItems(startupConsistencyCheck.getInconsistentUris())
          .setLiveWorkerNodes(mBlockMaster.getWorkerCount())
          .setMasterNodeAddress(mMasterProcess.getRpcAddress().toString())
          .setStartTime(CommonUtils.convertMsToDate(mMetaMaster.getStartTimeMs()))
          .setStorageTierInfos(infos).setUptime(CommonUtils
              .convertMsToClockTime(System.currentTimeMillis() - mMetaMaster.getStartTimeMs()))
          .setUsedCapacity(FormatUtils.getSizeFromBytes(mBlockMaster.getUsedBytes()))
          .setVersion(RuntimeConstants.VERSION);
    });
  }

  /**
   * Get the information required for the UI's browse page
   * @param requestPath
   * @param requestOffset
   * @param requestEnd
   * @param requestLimit
   * @return
   */
  @GET
  @Path(WEBUI_BROWSE)
  @ReturnType("alluxio.wire.MasterWebUIBrowse")
  public Response getWebUIBrowse(@DefaultValue("/") @QueryParam("path") String requestPath,
      @DefaultValue("0") @QueryParam("offset") String requestOffset,
      @QueryParam("end") String requestEnd,
      @DefaultValue("20") @QueryParam("limit") String requestLimit) {
    return RestUtils.call(() -> {
      if (!Configuration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return new MasterWebUIBrowse();
      }

      if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
        AuthenticatedClientUser.set(LoginUser.get().getName());
      }

      AlluxioURI currentPath = new AlluxioURI(requestPath);
      List<FileInfo> filesInfo = null;
      List<UIFileBlockInfo> fileBlocks = null;
      long offset = 0;
      long viewingOffset = offset;
      String accessControlException = "";
      String blockSizeBytes = "";
      String fatalError = "";
      String fileData = "";
      String fileDoesNotExistException = "";
      String highestTierAlias = "";
      String invalidPathError = "";
      String invalidPathException = "";
      UIFileInfo currentDirectory = null;
      UIFileInfo[] pathInfos = null;

      try {
        long fileId = mFileSystemMaster.getFileId(currentPath);
        FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
        UIFileInfo currentFileInfo = new UIFileInfo(fileInfo);
        if (currentFileInfo.getAbsolutePath() == null) {
          invalidPathError = "Error: Invalid Path " + new FileDoesNotExistException(currentPath.toString()).getMessage();
        }
        currentDirectory = currentFileInfo;
        blockSizeBytes = currentFileInfo.getBlockSizeBytes();
        if (!currentFileInfo.getIsDirectory()) {
          long relativeOffset = 0;
          try {
            relativeOffset = Long.parseLong(requestOffset);
          } catch (NumberFormatException e) {
            relativeOffset = 0;
          }
          String endParam = requestEnd;
          // If no param "end" presents, the offset is relative to the beginning; otherwise, it is
          // relative to the end of the file.
          if (endParam == null) {
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
            AlluxioURI path = new AlluxioURI(currentFileInfo.getAbsolutePath());
            FileSystem fs = FileSystem.Factory.get();
            URIStatus status = fs.getStatus(path);
            if (status.isCompleted()) {
              OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
              try (FileInStream is = fs.openFile(path, options)) {
                int len = (int) Math.min(5 * Constants.KB, status.getLength() - offset);
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
            for (FileBlockInfo fileBlockInfo : mMasterProcess.getMaster(FileSystemMaster.class)
                .getFileBlockInfoList(path)) {
              uiBlockInfo.add(new UIFileBlockInfo(fileBlockInfo));
            }
            fileBlocks = uiBlockInfo;
            highestTierAlias =
                mMasterProcess.getMaster(BlockMaster.class).getGlobalStorageTierAssoc().getAlias(0);
          } catch (AlluxioException e) {
            fatalError = "Error: Fatal Error " + e.getMessage();
          }
          viewingOffset = offset;
        }

        AlluxioURI path = new AlluxioURI(currentFileInfo.getAbsolutePath());
        if (path.isRoot()) {
          pathInfos = new UIFileInfo[0];
        } else {
          String[] splitPath = PathUtils.getPathComponents(path.toString());
          pathInfos = new UIFileInfo[splitPath.length - 1];
          currentPath = new AlluxioURI(AlluxioURI.SEPARATOR);
          fileId = mFileSystemMaster.getFileId(currentPath);
          pathInfos[0] = new UIFileInfo(mFileSystemMaster.getFileInfo(fileId));
          for (int i = 1; i < splitPath.length - 1; i++) {
            currentPath = currentPath.join(splitPath[i]);
            fileId = mFileSystemMaster.getFileId(currentPath);
            pathInfos[i] = new UIFileInfo(mFileSystemMaster.getFileInfo(fileId));
          }
        }

        filesInfo = mFileSystemMaster.listStatus(currentPath,
            ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always));
      } catch (FileDoesNotExistException e) {
        invalidPathError = "Error: Invalid Path " + e.getMessage();
      } catch (InvalidPathException e) {
        invalidPathError = "Error: Invalid Path " + e.getLocalizedMessage();
      } catch (UnavailableException e) {
        invalidPathError = "The service is temporarily unavailable. " + e.getMessage();
      } catch (IOException e) {
        invalidPathError = "Error: File " + currentPath + " is not available " + e.getMessage();
      } catch (AccessControlException e) {
        invalidPathError = "Error: File " + currentPath + " cannot be accessed " + e.getMessage();
      }

      List<UIFileInfo> fileInfos = new ArrayList<>(filesInfo.size());
      for (FileInfo fileInfo : filesInfo) {
        UIFileInfo toAdd = new UIFileInfo(fileInfo);
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
          fileDoesNotExistException = "Error: non-existing file " + e.getMessage();
        } catch (InvalidPathException e) {
          invalidPathException = "Error: invalid path " + e.getMessage();
        } catch (AccessControlException e) {
          accessControlException =
              "Error: File " + currentPath + " cannot be accessed " + e.getMessage();
        }
        fileInfos.add(toAdd);
      }
      if (fileInfos.size() > 1) {
        Collections.sort(fileInfos, UIFileInfo.PATH_STRING_COMPARE);
      }

      try {
        offset = Long.parseLong(requestOffset);
        offset = offset > Integer.MAX_VALUE ? Integer.MAX_VALUE : offset;
        offset = offset < Integer.MIN_VALUE ? Integer.MIN_VALUE : offset;
        int limit = Integer.parseInt(requestLimit);
        limit = limit > fileInfos.size() ? fileInfos.size() : limit;
        long sum = Math.addExact(offset, limit); // should throw an exception in case of overflow
        sum = sum > Integer.MAX_VALUE ? Integer.MAX_VALUE : sum;
        fileInfos = fileInfos.subList((int) offset, (int) sum);
      } catch (NumberFormatException e) {
        fatalError = "Error: offset or limit parse error, " + e.getLocalizedMessage();
      } catch (IndexOutOfBoundsException e) {
        fatalError = "Error: offset or offset + limit is out of bound, " + e.getLocalizedMessage();
      } catch (IllegalArgumentException e) {
        fatalError = e.getLocalizedMessage();
      }

      return new MasterWebUIBrowse().setAccessControlException(accessControlException)
          .setBlockSizeBytes(blockSizeBytes).setCurrentDirectory(currentDirectory)
          .setCurrentPath(currentPath.toString()).setDebug(getBoolean(PropertyKey.DEBUG))
          .setFatalError(fatalError).setFileBlocks(fileBlocks).setFileData(fileData)
          .setFileDoesNotExistException(fileDoesNotExistException)
          .setHighestTierAlias(highestTierAlias).setInvalidPathError(invalidPathError)
          .setInvalidPathException(invalidPathException)
          .setMasterNodeAddress(mMasterProcess.getRpcAddress().toString())
          .setNTotalFile(fileInfos == null ? 0 : fileInfos.size()).setPathInfos(pathInfos)
          .setShowPermissions(getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED))
          .setViewingOffset(viewingOffset).setFileInfos(fileInfos);
    });
  }

  @GET
  @Path(WEBUI_DATA)
  @ReturnType("alluxio.wire.MasterWebUIData")
  public Response getWebUIData(@DefaultValue("0") @QueryParam("offset") String requestOffset,
      @DefaultValue("20") @QueryParam("limit") String requestLimit) {
    return RestUtils.call(() -> {
      if (!Configuration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return new MasterWebUIData();
      }
      if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
        AuthenticatedClientUser.set(LoginUser.get().getName());
      }

      String fatalError = "";
      String permissionError = "";
      List<AlluxioURI> inAlluxioFiles = mFileSystemMaster.getInAlluxioFiles();
      Collections.sort(inAlluxioFiles);
      int inAlluxioFileNum;

      List<UIFileInfo> fileInfos = new ArrayList<>(inAlluxioFiles.size());
      for (AlluxioURI file : inAlluxioFiles) {
        try {
          long fileId = mFileSystemMaster.getFileId(file);
          FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
          if (fileInfo != null && fileInfo.getInAlluxioPercentage() == 100) {
            fileInfos.add(new UIFileInfo(fileInfo));
          }
        } catch (FileDoesNotExistException e) {
          fatalError = "Error: File does not exist " + e.getLocalizedMessage();
        } catch (AccessControlException e) {
          permissionError = "Error: File " + file + " cannot be accessed " + e.getMessage();
        }
      }
      inAlluxioFileNum = fileInfos.size();

      try {
        int offset = Integer.parseInt(requestOffset);
        int limit = Integer.parseInt(requestLimit);
        limit = limit > fileInfos.size() ? fileInfos.size() : limit;
        int sum = Math.addExact(offset, limit);
        fileInfos = fileInfos.subList(offset, sum);
      } catch (NumberFormatException e) {
        fatalError = "Error: offset or limit parse error, " + e.getLocalizedMessage();
      } catch (IndexOutOfBoundsException e) {
        fatalError = "Error: offset or offset + limit is out of bound, " + e.getLocalizedMessage();
      } catch (IllegalArgumentException e) {
        fatalError = e.getLocalizedMessage();
      }

      return new MasterWebUIData().setMasterNodeAddress(mMasterProcess.getRpcAddress().toString())
          .setFatalError(fatalError).setPermissionError(permissionError)
          .setShowPermissions(getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED))
          .setInAlluxioFileNum(inAlluxioFileNum).setFileInfos(fileInfos);
    });
  }

  @GET
  @Path(WEBUI_LOGS)
  @ReturnType("alluxio.wire.MasterWebUILogs")
  public Response getWebUILogs(@DefaultValue("") @QueryParam("path") String requestPath,
      @DefaultValue("0") @QueryParam("offset") String requestOffset,
      @QueryParam("end") String requestEnd,
      @DefaultValue("20") @QueryParam("limit") String requestLimit) {
    return RestUtils.call(() -> {
      if (!Configuration.getBoolean(PropertyKey.WEB_FILE_INFO_ENABLED)) {
        return new MasterWebUILogs();
      }

      FilenameFilter LOG_FILE_FILTER = (dir, name) -> name.toLowerCase().endsWith(".log");
      boolean debug = getBoolean(PropertyKey.DEBUG);
      String invalidPathError = "";
      long viewingOffset = 0;
      String currentPath = "";
      int nTotalFile = 0;
      List<UIFileInfo> fileInfos = new ArrayList<>();
      String fatalError = "";
      String logsPath = Configuration.get(PropertyKey.LOGS_DIR);
      File logsDir = new File(logsPath);
      String fileData = "";

      if (requestPath == null || requestPath.isEmpty()) {
        // List all log files in the log/ directory.

        File[] logFiles = logsDir.listFiles(LOG_FILE_FILTER);
        if (logFiles != null) {
          for (File logFile : logFiles) {
            String logFileName = logFile.getName();
            fileInfos.add(new UIFileInfo(
                new UIFileInfo.LocalFileInfo(logFileName, logFileName, logFile.length(),
                    UIFileInfo.LocalFileInfo.EMPTY_CREATION_TIME, logFile.lastModified(),
                    logFile.isDirectory())));
          }
        }
        Collections.sort(fileInfos, UIFileInfo.PATH_STRING_COMPARE);
        nTotalFile = fileInfos.size();

        try {
          int offset = Integer.parseInt(requestOffset);
          int limit = Integer.parseInt(requestLimit);
          fileInfos = fileInfos.subList(offset, offset + limit);
        } catch (NumberFormatException e) {
          fatalError = "Error: offset or limit parse error, " + e.getLocalizedMessage();
        } catch (IndexOutOfBoundsException e) {
          fatalError =
              "Error: offset or offset + limit is out of bound, " + e.getLocalizedMessage();
        } catch (IllegalArgumentException e) {
          fatalError = e.getLocalizedMessage();
        }
      } else {
        // Request a specific log file.

        // Only allow filenames as the path, to avoid arbitrary local path lookups.
        String requestFile = new File(requestPath).getName();
        currentPath = requestFile;

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

          try (InputStream is = new FileInputStream(logFile)) {
            fileSize = logFile.length();
            int len = (int) Math.min(5 * Constants.KB, fileSize - offset);
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

          viewingOffset = offset;
        } catch (IOException e) {
          invalidPathError = "Error: File " + logFile + " is not available " + e.getMessage();
        }
      }

      return new MasterWebUILogs().setCurrentPath(currentPath).setDebug(debug).setFatalError(fatalError)
          .setFileData(fileData).setFileInfos(fileInfos).setInvalidPathError(invalidPathError)
          .setNTotalFile(nTotalFile).setViewingOffset(viewingOffset);
    });
  }

  @GET
  @Path(WEBUI_CONFIG)
  @ReturnType("alluxio.wire.MasterWebUIConfiguration")
  public Response getWebUIConfiguration() {
    return RestUtils.call(() -> {
      TreeSet<Triple<String, String, String>> sortedProperties = new TreeSet<>();
      Set<String> alluxioConfExcludes = Sets.newHashSet(PropertyKey.MASTER_WHITELIST.toString());
      for (ConfigProperty configProperty : mMetaMaster
          .getConfiguration(GetConfigurationOptions.defaults().setRawValue(true))) {
        String confName = configProperty.getName();
        if (!alluxioConfExcludes.contains(confName)) {
          sortedProperties.add(new ImmutableTriple<>(confName,
              ConfigurationUtils.valueAsString(configProperty.getValue()),
              configProperty.getSource()));
        }
      }

      return new MasterWebUIConfiguration().setWhitelist(mFileSystemMaster.getWhiteList())
          .setConfiguration(sortedProperties);
    });
  }

  @GET
  @Path(WEBUI_WORKERS)
  @ReturnType("alluxio.wire.MasterWebUIWorkers")
  public Response getWebUIWorkers() {
    return RestUtils.call(() -> {
      List<WorkerInfo> workerInfos = mBlockMaster.getWorkerInfoList();
      NodeInfo[] normalNodeInfos = WebUtils.generateOrderedNodeInfos(workerInfos);

      List<WorkerInfo> lostWorkerInfos = mBlockMaster.getLostWorkersInfoList();
      NodeInfo[] failedNodeInfos = WebUtils.generateOrderedNodeInfos(lostWorkerInfos);

      return new MasterWebUIWorkers().setDebug(getBoolean(PropertyKey.DEBUG))
          .setNormalNodeInfos(normalNodeInfos).setFailedNodeInfos(failedNodeInfos);
    });
  }

  @GET
  @Path(WEBUI_METRICS)
  @ReturnType("alluxio.wire.MasterWebUIMetrics")
  public Response getWebUIMetrics() {
    return RestUtils.call(() -> {
      MetricRegistry mr = MetricsSystem.METRIC_REGISTRY;

      Long masterCapacityTotal = (Long) mr.getGauges()
          .get(MetricsSystem.getMetricName(DefaultBlockMaster.Metrics.CAPACITY_TOTAL)).getValue();
      Long masterCapacityUsed = (Long) mr.getGauges()
          .get(MetricsSystem.getMetricName(DefaultBlockMaster.Metrics.CAPACITY_USED)).getValue();

      int masterCapacityUsedPercentage =
          (masterCapacityTotal > 0) ? (int) (100L * masterCapacityUsed / masterCapacityTotal) : 0;

      Long masterUnderfsCapacityTotal =
          (Long) mr.getGauges().get(MetricsSystem.getMetricName(MasterMetrics.UFS_CAPACITY_TOTAL))
              .getValue();
      Long masterUnderfsCapacityUsed =
          (Long) mr.getGauges().get(MetricsSystem.getMetricName(MasterMetrics.UFS_CAPACITY_USED))
              .getValue();

      int masterUnderfsCapacityUsedPercentage = (masterUnderfsCapacityTotal > 0) ?
          (int) (100L * masterUnderfsCapacityUsed / masterUnderfsCapacityTotal) : 0;

      // cluster read size
      Long bytesReadLocal = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL)).getValue();
      Long bytesReadRemote = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO)).getValue();
      Long bytesReadUfs = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_ALL)).getValue();

      // cluster cache hit and miss
      long bytesReadTotal = bytesReadLocal + bytesReadRemote + bytesReadUfs;
      double cacheHitLocalPercentage =
          (bytesReadTotal > 0) ? (100D * bytesReadLocal / bytesReadTotal) : 0;
      double cacheHitRemotePercentage =
          (bytesReadTotal > 0) ? (100D * bytesReadRemote / bytesReadTotal) : 0;
      double cacheMissPercentage =
          (bytesReadTotal > 0) ? (100D * bytesReadUfs / bytesReadTotal) : 0;

      // cluster write size
      Long bytesWrittenAlluxio = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO)).getValue();
      Long bytesWrittenUfs = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_ALL)).getValue();

      // cluster read throughput
      Long bytesReadLocalThroughput = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT))
          .getValue();
      Long bytesReadRemoteThroughput = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_ALLUXIO_THROUGHPUT))
          .getValue();
      Long bytesReadUfsThroughput = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_READ_UFS_THROUGHPUT))
          .getValue();

      // cluster write throughput
      Long bytesWrittenAlluxioThroughput = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_ALLUXIO_THROUGHPUT))
          .getValue();
      Long bytesWrittenUfsThroughput = (Long) mr.getGauges()
          .get(MetricsSystem.getClusterMetricName(WorkerMetrics.BYTES_WRITTEN_UFS_THROUGHPUT))
          .getValue();

      // cluster per UFS read
      Map<String, String> ufsReadSizeMap = new TreeMap<>();
      for (Map.Entry<String, Gauge> entry : mr
          .getGauges((name, metric) -> name.contains(WorkerMetrics.BYTES_READ_UFS)).entrySet()) {
        alluxio.metrics.Metric metric =
            alluxio.metrics.Metric.from(entry.getKey(), (long) entry.getValue().getValue());
        ufsReadSizeMap.put(metric.getTags().get(WorkerMetrics.TAG_UFS),
            FormatUtils.getSizeFromBytes((long) metric.getValue()));
      }

      // cluster per UFS write
      Map<String, String> ufsWriteSizeMap = new TreeMap<>();
      for (Map.Entry<String, Gauge> entry : mr
          .getGauges((name, metric) -> name.contains(WorkerMetrics.BYTES_WRITTEN_UFS)).entrySet()) {
        alluxio.metrics.Metric metric =
            alluxio.metrics.Metric.from(entry.getKey(), (long) entry.getValue().getValue());
        ufsWriteSizeMap.put(metric.getTags().get(WorkerMetrics.TAG_UFS),
            FormatUtils.getSizeFromBytes((long) metric.getValue()));
      }

      // per UFS ops
      Map<String, Map<String, Long>> ufsOpsMap = new TreeMap<>();
      for (Map.Entry<String, Gauge> entry : mr
          .getGauges((name, metric) -> name.contains(WorkerMetrics.UFS_OP_PREFIX)).entrySet()) {
        alluxio.metrics.Metric metric =
            alluxio.metrics.Metric.from(entry.getKey(), (long) entry.getValue().getValue());
        if (!metric.getTags().containsKey(WorkerMetrics.TAG_UFS)) {
          continue;
        }
        String ufs = metric.getTags().get(WorkerMetrics.TAG_UFS);
        Map<String, Long> perUfsMap = ufsOpsMap.getOrDefault(ufs, new TreeMap<>());
        perUfsMap.put(metric.getName().replaceFirst(WorkerMetrics.UFS_OP_PREFIX, ""),
            (long) metric.getValue());
        ufsOpsMap.put(ufs, perUfsMap);
      }

      Map<String, Counter> counters = mr.getCounters(new MetricFilter() {
        @Override
        public boolean matches(String name, Metric metric) {
          return !(name.endsWith("Ops"));
        }
      });

      Map<String, Counter> rpcInvocations = mr.getCounters(new MetricFilter() {
        @Override
        public boolean matches(String name, Metric metric) {
          return name.endsWith("Ops");
        }
      });

      Map<String, Metric> operations = new TreeMap<>();
      // Remove the instance name from the metrics.
      for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        operations.put(MetricsSystem.stripInstanceAndHost(entry.getKey()), entry.getValue());
      }
      String filesPinnedProperty = MetricsSystem.getMetricName(MasterMetrics.FILES_PINNED);
      operations.put(MetricsSystem.stripInstanceAndHost(filesPinnedProperty),
          mr.getGauges().get(filesPinnedProperty));

      Map<String, Counter> rpcInvocationsUpdated = new TreeMap<>();
      for (Map.Entry<String, Counter> entry : rpcInvocations.entrySet()) {
        rpcInvocationsUpdated
            .put(MetricsSystem.stripInstanceAndHost(entry.getKey()), entry.getValue());
      }

      return new MasterWebUIMetrics().setMasterCapacityUsedPercentage(masterCapacityUsedPercentage)
          .setMasterCapacityFreePercentage(100 - masterUnderfsCapacityUsedPercentage)
          .setMasterUnderfsCapacityUsedPercentage(masterUnderfsCapacityUsedPercentage)
          .setMasterUnderfsCapacityFreePercentage(100 - masterUnderfsCapacityUsedPercentage)
          .setTotalBytesReadLocal(FormatUtils.getSizeFromBytes(bytesReadLocal))
          .setTotalBytesReadRemote(FormatUtils.getSizeFromBytes(bytesReadRemote))
          .setTotalBytesReadUfs(FormatUtils.getSizeFromBytes(bytesReadUfs))
          .setCacheHitLocal(String.format("%.2f", cacheHitLocalPercentage))
          .setCacheHitRemote(String.format("%.2f", cacheHitRemotePercentage))
          .setCacheMiss(String.format("%.2f", cacheMissPercentage))
          .setTotalBytesWrittenAlluxio(FormatUtils.getSizeFromBytes(bytesWrittenAlluxio))
          .setTotalBytesWrittenUfs(FormatUtils.getSizeFromBytes(bytesWrittenUfs))
          .setTotalBytesReadLocalThroughput(FormatUtils.getSizeFromBytes(bytesReadLocalThroughput))
          .setTotalBytesReadRemoteThroughput(
              FormatUtils.getSizeFromBytes(bytesReadRemoteThroughput))
          .setTotalBytesReadUfsThroughput(FormatUtils.getSizeFromBytes(bytesReadUfsThroughput))
          .setTotalBytesWrittenAlluxioThroughput(
              FormatUtils.getSizeFromBytes(bytesWrittenAlluxioThroughput))
          .setTotalBytesWrittenUfsThroughput(
              FormatUtils.getSizeFromBytes(bytesWrittenUfsThroughput))
          .setUfsReadSize(ufsReadSizeMap).setUfsWriteSize(ufsWriteSizeMap).setUfsOps(ufsOpsMap)
          .setOperationMetrics(operations).setRpcInvocationMetrics(rpcInvocations);
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
    return RestUtils.call(this::getMetricsInternal);
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
    return RestUtils.call(() -> mMasterProcess.getStartTimeMs());
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
    return RestUtils.call(() -> getUfsCapacityInternal().getTotal());
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
    return RestUtils.call(() -> getUfsCapacityInternal().getUsed());
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
    return RestUtils.call(() -> {
      Capacity capacity = getUfsCapacityInternal();
      if (capacity.getTotal() >= 0 && capacity.getUsed() >= 0 && capacity.getTotal() >= capacity
          .getUsed()) {
        return capacity.getTotal() - capacity.getUsed();
      }
      return -1;
    });
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
    return RestUtils.call(() -> mBlockMaster.getWorkerCount());
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
    return RestUtils.call(() -> mBlockMaster.getWorkerInfoList());
  }

  private Capacity getCapacityInternal() {
    return new Capacity().setTotal(mBlockMaster.getCapacityBytes())
        .setUsed(mBlockMaster.getUsedBytes());
  }

  private Map<String, String> getConfigurationInternal(boolean raw) {
    return new TreeMap<>(Configuration
        .toMap(ConfigurationValueOptions.defaults().useDisplayValue(true).useRawValue(raw)));
  }

  private Map<String, Long> getMetricsInternal() {
    MetricRegistry metricRegistry = MetricsSystem.METRIC_REGISTRY;

    // Get all counters.
    Map<String, Counter> counters = metricRegistry.getCounters();
    // Only the gauge for pinned files is retrieved here, other gauges are statistics of
    // free/used
    // spaces, those statistics can be gotten via other REST apis.
    String filesPinnedProperty = MetricsSystem.getMetricName(MasterMetrics.FILES_PINNED);
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

  private Capacity getUfsCapacityInternal() {
    MountPointInfo mountInfo = mFileSystemMaster.getMountTable().get(MountTable.ROOT);
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
  @ReturnType("alluxio.wire.LogInfo")
  public Response logLevel(@QueryParam(LOG_ARGUMENT_NAME) final String logName,
      @QueryParam(LOG_ARGUMENT_LEVEL) final String level) {
    return RestUtils.call(() -> LogUtils.setLogLevel(logName, level));
  }
}
