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

package alluxio.client.rest;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.meta.AlluxioMasterRestServiceHandler;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.StartupConsistencyCheck;
import alluxio.metrics.MetricsSystem;
import alluxio.testutils.master.MasterTestUtils;
import alluxio.testutils.underfs.UnderFileSystemTestUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.AlluxioMasterInfo;
import alluxio.wire.Capacity;
import alluxio.wire.MasterWebUIBrowse;
import alluxio.wire.MasterWebUIData;
import alluxio.wire.MasterWebUIInit;
import alluxio.wire.MasterWebUILogs;
import alluxio.wire.MasterWebUIMetrics;
import alluxio.wire.MasterWebUIOverview;
import alluxio.wire.MasterWebUIWorkers;
import alluxio.wire.MountPointInfo;
import alluxio.wire.WorkerInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link AlluxioMasterRestServiceHandler}.
 */
public final class AlluxioMasterRestApiTest extends RestApiTest {
  private FileSystemMaster mFileSystemMaster;
  private int mRpcPort;
  private int mWorkerPort;
  private int mProxyWebPort;

  @Before
  public void before() {
    mFileSystemMaster = mResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getLocalAlluxioMaster().getMasterProcess().getWebAddress().getPort();
    mRpcPort = mResource.get().getLocalAlluxioMaster().getAddress().getPort();
    mWorkerPort = mResource.get().getWorkerProcess().getAddress().getWebPort();
    mProxyWebPort = mResource.get().getProxyProcess().getWebLocalPort();
    mServicePrefix = AlluxioMasterRestServiceHandler.SERVICE_PREFIX;

    MetricsSystem.resetAllCounters();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  private AlluxioMasterInfo getInfo(Map<String, String> params) throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.GET_INFO),
            params, HttpMethod.GET, null).call();
    AlluxioMasterInfo info = new ObjectMapper().readValue(result, AlluxioMasterInfo.class);
    return info;
  }

  @Test
  public void getCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo(NO_PARAMS).getCapacity();
    Assert.assertEquals(total, capacity.getTotal());
    Assert.assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getConfiguration() throws Exception {
    String home = "home";
    String rawConfDir = String.format("${%s}/conf", PropertyKey.Name.HOME);
    String resolvedConfDir = String.format("%s/conf", home);
    ServerConfiguration.set(PropertyKey.HOME, home);
    ServerConfiguration.set(PropertyKey.CONF_DIR, rawConfDir);

    // with out any query parameter, configuration values are resolved.
    checkConfiguration(PropertyKey.CONF_DIR, resolvedConfDir, NO_PARAMS);

    // with QUERY_RAW_CONFIGURATION=false, configuration values are resolved.
    Map<String, String> params = new HashMap<>();
    params.put(AlluxioMasterRestServiceHandler.QUERY_RAW_CONFIGURATION, "false");
    checkConfiguration(PropertyKey.CONF_DIR, resolvedConfDir, params);

    // with QUERY_RAW_CONFIGURATION=true, configuration values are raw.
    params.put(AlluxioMasterRestServiceHandler.QUERY_RAW_CONFIGURATION, "true");
    checkConfiguration(PropertyKey.CONF_DIR, rawConfDir, params);
  }

  private void checkConfiguration(PropertyKey key, String expectedValue, Map<String, String> params)
      throws Exception {
    Assert.assertEquals(expectedValue, getInfo(params).getConfiguration().get(key.toString()));
  }

  @Test
  public void getLostWorkers() throws Exception {
    List<WorkerInfo> lostWorkersInfo = getInfo(NO_PARAMS).getLostWorkers();
    Assert.assertEquals(0, lostWorkersInfo.size());
  }

  @Test
  public void getMetrics() throws Exception {
    Assert.assertEquals(Long.valueOf(0),
        getInfo(NO_PARAMS).getMetrics().get(MetricsSystem.getMetricName("CompleteFileOps")));
  }

  @Test
  public void getMountPoints() throws Exception {
    Map<String, MountPointInfo> mountTable = mFileSystemMaster.getMountTable();
    Map<String, MountPointInfo> mountPoints = getInfo(NO_PARAMS).getMountPoints();
    Assert.assertEquals(mountTable.size(), mountPoints.size());
    for (Map.Entry<String, MountPointInfo> mountPoint : mountTable.entrySet()) {
      Assert.assertTrue(mountPoints.containsKey(mountPoint.getKey()));
      String expectedUri = mountPoints.get(mountPoint.getKey()).getUfsUri();
      String returnedUri = mountPoint.getValue().getUfsUri();
      Assert.assertEquals(expectedUri, returnedUri);
    }
  }

  @Test
  public void getRpcAddress() throws Exception {
    Assert.assertTrue(getInfo(NO_PARAMS).getRpcAddress().contains(String.valueOf(
        NetworkAddressUtils.getPort(ServiceType.MASTER_RPC, ServerConfiguration.global()))));
  }

  @Test
  public void getStartTimeMs() throws Exception {
    Assert.assertTrue(getInfo(NO_PARAMS).getStartTimeMs() > 0);
  }

  @Test
  public void getStartupConsistencyCheckStatus() throws Exception {
    MasterTestUtils.waitForStartupConsistencyCheck(mFileSystemMaster);
    alluxio.wire.StartupConsistencyCheck status = getInfo(NO_PARAMS).getStartupConsistencyCheck();
    Assert.assertEquals(StartupConsistencyCheck.Status.COMPLETE.toString().toLowerCase(),
        status.getStatus());
    Assert.assertEquals(0, status.getInconsistentUris().size());
  }

  @Test
  public void getTierCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo(NO_PARAMS).getTierCapacity().get("MEM");
    Assert.assertEquals(total, capacity.getTotal());
    Assert.assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getUptimeMs() throws Exception {
    Assert.assertTrue(getInfo(NO_PARAMS).getUptimeMs() > 0);
  }

  @Test
  public void getUfsCapacity() throws Exception {
    Capacity ufsCapacity = getInfo(NO_PARAMS).getUfsCapacity();
    if (UnderFileSystemTestUtils.isObjectStorage(mFileSystemMaster.getUfsAddress())) {
      // Object storage ufs capacity is always invalid.
      Assert.assertEquals(-1, ufsCapacity.getTotal());
    } else {
      Assert.assertTrue(ufsCapacity.getTotal() > 0);
    }
  }

  @Test
  public void getWorkers() throws Exception {
    List<WorkerInfo> workerInfos = getInfo(NO_PARAMS).getWorkers();
    Assert.assertEquals(1, workerInfos.size());
    WorkerInfo workerInfo = workerInfos.get(0);
    Assert.assertEquals(0, workerInfo.getUsedBytes());
    long bytes = ServerConfiguration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Assert.assertEquals(bytes, workerInfo.getCapacityBytes());
  }

  @Test
  public void getVersion() throws Exception {
    Assert.assertEquals(RuntimeConstants.VERSION, getInfo(NO_PARAMS).getVersion());
  }

  private MasterWebUIBrowse getWebUIBrowseData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_BROWSE),
            NO_PARAMS, HttpMethod.GET, null).call();
    MasterWebUIBrowse data = new ObjectMapper().readValue(result, MasterWebUIBrowse.class);
    return data;
  }

  @Test
  public void WebUIBrowse() throws Exception {
    MasterWebUIBrowse result = getWebUIBrowseData();
    String expectedJson = ("{`debug`:false,`accessControlException`:null,`blockSizeBytes`:``,"
        + "`currentDirectory`:{`id`:0,`name`:`root`,`absolutePath`:`/`,`blockSizeBytes`:``,"
        + "`size`:``,`inAlluxio`:false,`inAlluxioPercentage`:0,`isDirectory`:true,"
        + "`pinned`:false,`owner`:``,`group`:``,`mode`:`drwxr-xr-x`,"
        + "`persistenceState`:`PERSISTED`,`fileLocations`:[],`blocksOnTier`:{}},"
        + "`currentPath`:`/`,`fatalError`:null,`fileBlocks`:null,`fileData`:null,"
        + "`fileDoesNotExistException`:null,`fileInfos`:[],`highestTierAlias`:null,"
        + "`invalidPathError`:``,`invalidPathException`:null,`masterNodeAddress`:`/" + mHostname
        + ":" + mRpcPort
        + "`,`ntotalFile`:0,`pathInfos`:[],`showPermissions`:false,`viewingOffset`:0}")
        .replace('`', '"');
    MasterWebUIBrowse expected =
        new ObjectMapper().readValue(expectedJson, MasterWebUIBrowse.class);
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
    String resultString = new ObjectMapper().writer().writeValueAsString(result);
    Assert.assertEquals(expectedString, resultString);
  }

  //    private MasterWebUIConfiguration getWebUIConfigurationData() throws Exception {
  //      String result = new TestCase(mHostname, mPort,
  //          getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_CONFIG), NO_PARAMS,
  //          HttpMethod.GET, null)
  //          .call();
  //      MasterWebUIConfiguration data = new ObjectMapper().readValue(result,
  //      MasterWebUIConfiguration.class);
  //      return data;
  //    }
  //
  //    @Test
  //    public void WebUIConfiguration() throws Exception {
  //      MasterWebUIConfiguration result = getWebUIConfigurationData();
  //      String expectedJson =
  //          ("{}").replace('`', '"');
  //      MasterWebUIConfiguration expected = new ObjectMapper().readValue(expectedJson,
  //      MasterWebUIConfiguration.class);
  //      String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
  //      String resultString = new ObjectMapper().writer().writeValueAsString(result);
  //      Assert.assertEquals(expectedString, resultString);
  //    }

  private MasterWebUIData getWebUIDataData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_DATA),
            NO_PARAMS, HttpMethod.GET, null).call();
    MasterWebUIData data = new ObjectMapper().readValue(result, MasterWebUIData.class);
    return data;
  }

  @Test
  public void WebUIData() throws Exception {
    MasterWebUIData result = getWebUIDataData();
    String expectedJson =
        ("{`fatalError`:``,`fileInfos`:[],`masterNodeAddress`:`/" + mHostname + ":" + mRpcPort
            + "`,`showPermissions`:false,`inAlluxioFileNum`:0,`permissionError`:null}")
            .replace('`', '"');
    MasterWebUIData expected = new ObjectMapper().readValue(expectedJson, MasterWebUIData.class);
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
    String resultString = new ObjectMapper().writer().writeValueAsString(result);
    Assert.assertEquals(expectedString, resultString);
  }

  private MasterWebUIInit getWebUIInitData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_INIT),
            NO_PARAMS, HttpMethod.GET, null).call();
    MasterWebUIInit data = new ObjectMapper().readValue(result, MasterWebUIInit.class);
    return data;
  }

  @Test
  public void WebUIInit() throws Exception {
    MasterWebUIInit result = getWebUIInitData();
    String expectedJson = ("{`debug`:false,`webFileInfoEnabled`:true,"
        + "`securityAuthorizationPermissionEnabled`:false,`workerPort`:0,"
        + "`refreshInterval`:15000,`proxyDownloadFileApiUrl`:{`prefix`:`http://" + mHostname + ":"
        + mProxyWebPort + "/api/v1/paths/`,`suffix`:`/download-file/`}}").replace('`', '"');
    MasterWebUIInit expected = new ObjectMapper().readValue(expectedJson, MasterWebUIInit.class);
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
    String resultString = new ObjectMapper().writer().writeValueAsString(result);
    Assert.assertEquals(expectedString, resultString);
  }

  private MasterWebUILogs getWebUILogsData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_LOGS),
            NO_PARAMS, HttpMethod.GET, null).call();
    MasterWebUILogs data = new ObjectMapper().readValue(result, MasterWebUILogs.class);
    return data;
  }

  @Test
  public void WebUILogs() throws Exception {
    MasterWebUILogs result = getWebUILogsData();
    String expectedJson =
        ("{`debug`:false,`currentPath`:``,`fatalError`:null,`fileData`:null,`fileInfos`:[],"
            + "`invalidPathError`:``,`ntotalFile`:0,`viewingOffset`:0}").replace('`', '"');
    MasterWebUILogs expected = new ObjectMapper().readValue(expectedJson, MasterWebUILogs.class);
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
    String resultString = new ObjectMapper().writer().writeValueAsString(result);
    Assert.assertEquals(expectedString, resultString);
  }

  private MasterWebUIMetrics getWebUIMetricsData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_METRICS),
            NO_PARAMS, HttpMethod.GET, null).call();
    MasterWebUIMetrics data = new ObjectMapper().readValue(result, MasterWebUIMetrics.class);
    return data;
  }

  @Test
  public void WebUIMetrics() throws Exception {
    MasterWebUIMetrics result = getWebUIMetricsData();
    String expectedJson = ("{`cacheHitLocal`:`0.00`,`cacheHitRemote`:`0.00`,`cacheMiss`:`0.00`,"
        + "`masterCapacityFreePercentage`:100,`masterCapacityUsedPercentage`:0,"
        + "`masterUnderfsCapacityFreePercentage`:51,`masterUnderfsCapacityUsedPercentage`:49,"
        + "`totalBytesReadLocal`:`0B`,`totalBytesReadLocalThroughput`:`0B`,"
        + "`totalBytesReadRemote`:`0B`,`totalBytesReadRemoteThroughput`:`0B`,"
        + "`totalBytesReadUfs`:`0B`,`totalBytesReadUfsThroughput`:`0B`,"
        + "`totalBytesWrittenAlluxio`:`0B`,`totalBytesWrittenAlluxioThroughput`:`0B`,"
        + "`totalBytesWrittenUfs`:`0B`,`totalBytesWrittenUfsThroughput`:`0B`,"
        + "`ufsOps`:{`_var_folders_zk_l642b_mn4097j57qt1wgk3mm0000gn_T_alluxio-tests_test"
        + "-cluster-cc76380c-c61f-46de-ba84-c94eaa257f8b_journal_BlockMaster`:{`Create`:1,"
        + "`IsDirectory`:1,`ListStatus`:10,`Mkdirs`:1},"
        + "`_var_folders_zk_l642b_mn4097j57qt1wgk3mm0000gn_T_alluxio-tests_test-cluster"
        + "-cc76380c-c61f-46de-ba84-c94eaa257f8b_journal_FileSystemMaster`:{`Create`:1,"
        + "`IsDirectory`:1,`ListStatus`:10,`Mkdirs`:1},"
        + "`_var_folders_zk_l642b_mn4097j57qt1wgk3mm0000gn_T_alluxio-tests_test-cluster"
        + "-cc76380c-c61f-46de-ba84-c94eaa257f8b_journal_MetaMaster`:{`Create`:1,"
        + "`IsDirectory`:1,`ListStatus`:10,`Mkdirs`:1},"
        + "`_var_folders_zk_l642b_mn4097j57qt1wgk3mm0000gn_T_alluxio-tests_test-cluster"
        + "-cc76380c-c61f-46de-ba84-c94eaa257f8b_journal_MetricsMaster`:{`Create`:1,"
        + "`IsDirectory`:1,`ListStatus`:10,`Mkdirs`:1},"
        + "`_var_folders_zk_l642b_mn4097j57qt1wgk3mm0000gn_T_alluxio-tests_test-cluster"
        + "-cc76380c-c61f-46de-ba84-c94eaa257f8b_underFSStorage`:{`IsDirectory`:2,"
        + "`Mkdirs`:1}},`ufsReadSize`:{},`ufsWriteSize`:{},"
        + "`operationMetrics`:{`AsyncCacheDuplicateRequests`:{`value`:0},"
        + "`AsyncCacheFailedBlocks`:{`value`:0},`AsyncCacheRemoteBlocks`:{`value`:0},"
        + "`AsyncCacheRequests`:{`value`:0},`AsyncCacheSucceededBlocks`:{`value`:0},"
        + "`AsyncCacheUfsBlocks`:{`value`:0},`BlocksAccessed`:{`value`:0},"
        + "`BlocksCanceled`:{`value`:0},`BlocksDeleted`:{`value`:0},"
        + "`BlocksEvicted`:{`value`:0},`BlocksLost`:{`value`:0},`BlocksPromoted`:{`value`:0},"
        + "`DirectoriesCreated`:{`value`:0},`FileBlockInfosGot`:{`value`:0},"
        + "`FileInfosGot`:{`value`:0},`FilesCompleted`:{`value`:0},"
        + "`FilesCreated`:{`value`:0},`FilesFreed`:{`value`:0},`FilesPersisted`:{`value`:0},"
        + "`FilesPinned`:{`value`:0},`NewBlocksGot`:{`value`:0},`PathsDeleted`:{`value`:0},"
        + "`PathsMounted`:{`value`:0},`PathsRenamed`:{`value`:0}," + "`PathsUnmounted`:{`value`:0},"
        + "`UfsSessionCount-Ufs:_var_folders_zk_l642b_mn4097j57qt1wgk3mm0000gn_T_alluxio"
        + "-tests_test-cluster-cc76380c-c61f-46de-ba84-c94eaa257f8b_underFSStorage`:{`value"
        + "`:0}},`rpcInvocationMetrics`:{`CompleteFileOps`:0,`CreateDirectoryOps`:0,"
        + "`CreateFileOps`:0,`DeletePathOps`:0,`FreeFileOps`:0,`GetFileBlockInfoOps`:0,"
        + "`GetFileInfoOps`:0,`GetNewBlockOps`:0,`MountOps`:0,`RenamePathOps`:0,"
        + "`SetAclOps`:0,`SetAttributeOps`:0,`UnmountOps`:0}}").replace('`', '"');
    MasterWebUIMetrics expected =
        new ObjectMapper().readValue(expectedJson, MasterWebUIMetrics.class);
    String findIgnored = "alluxio-tests_test-cluster-[^\"]+"
        + "(underFSStorage|journal_MetricsMaster|journal_MetaMaster|journal_FileSystemMaster"
        + "|journal_BlockMaster)";
    String replaceIgnored = "alluxio-tests-cluster_$1";
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected)
        .replaceAll(findIgnored, replaceIgnored);
    String resultString = new ObjectMapper().writer().writeValueAsString(result)
        .replaceAll(findIgnored, replaceIgnored);
    Assert.assertEquals(expectedString, resultString);
  }

  //  private MasterWebUIOverview getWebUIOverviewData() throws Exception {
  //    String result =
  //        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler
  //        .WEBUI_OVERVIEW),
  //            NO_PARAMS, HttpMethod.GET, null).call();
  //    MasterWebUIOverview data = new ObjectMapper().readValue(result, MasterWebUIOverview.class);
  //    return data;
  //  }
  //
  //  @Test
  //  public void WebUIOverview() throws Exception {
  //    MasterWebUIOverview result = getWebUIOverviewData();
  //    String expectedJson = ("{}").replace('`', '"');
  //    MasterWebUIOverview expected =
  //        new ObjectMapper().readValue(expectedJson, MasterWebUIOverview.class);
  //    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
  //    String resultString = new ObjectMapper().writer().writeValueAsString(result);
  //    Assert.assertEquals(expectedString, resultString);
  //  }

  private MasterWebUIWorkers getWebUIWorkersData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_WORKERS),
            NO_PARAMS, HttpMethod.GET, null).call();
    MasterWebUIWorkers data = new ObjectMapper().readValue(result, MasterWebUIWorkers.class);
    return data;
  }

  @Test
  public void WebUIWorkers() throws Exception {
    MasterWebUIWorkers result = getWebUIWorkersData();
    String expectedJson =
        ("{`debug`:false,`failedNodeInfos`:[],`normalNodeInfos`:[{`host`:`" + mHostname + "`,"
            + "`webPort`:" + mWorkerPort
            + ",`lastHeartbeat`:`0`,`state`:`In Service`,`capacity`:`0B`,"
            + "`usedMemory`:`0B`,`freeSpacePercent`:100,`usedSpacePercent`:0,`uptimeClockTime`:`0"
            + " d, 0 h, 0 m, and 0 s`,`workerId`:3426609678212409463}]}").replace('`', '"');
    MasterWebUIWorkers expected =
        new ObjectMapper().readValue(expectedJson, MasterWebUIWorkers.class);
    String findWorkerId =
        "`workerId`:\\d+".replace('`', '"'); // the worker id is a random 20 digit number
    String replaceWorkerId = "workerId: 00000000000000000000";
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected)
        .replaceAll(findWorkerId, replaceWorkerId);
    String resultString = new ObjectMapper().writer().writeValueAsString(result)
        .replaceAll(findWorkerId, replaceWorkerId);
    Assert.assertEquals(expectedString, resultString);
  }
}
