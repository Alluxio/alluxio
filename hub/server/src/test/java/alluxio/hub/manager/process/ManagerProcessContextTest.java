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

package alluxio.hub.manager.process;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.client.file.FileSystem;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.hub.common.HubUtil;
import alluxio.hub.manager.util.AlluxioCluster;
import alluxio.hub.manager.util.HubCluster;
import alluxio.hub.manager.util.HubTestUtils;
import alluxio.hub.proto.AgentFileUploadResponse;
import alluxio.hub.proto.AgentListCatalogResponse;
import alluxio.hub.proto.AgentListFileInfo;
import alluxio.hub.proto.AgentListFileResponse;
import alluxio.hub.proto.AgentManagerServiceGrpc;
import alluxio.hub.proto.AgentRemoveFileResponse;
import alluxio.hub.proto.AgentSetPrestoConfResponse;
import alluxio.hub.proto.AlluxioConfigurationSet;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.ApplyMountPointRequest;
import alluxio.hub.proto.ApplyMountPointResponse;
import alluxio.hub.proto.HubNodeAddress;
import alluxio.hub.proto.ListFile;
import alluxio.hub.proto.ListMountPointResponse;
import alluxio.hub.proto.PrestoCatalogListing;
import alluxio.hub.proto.PrestoCatalogListingResult;
import alluxio.hub.proto.RemoveFile;
import alluxio.hub.proto.SetPrestoConfDirRequest;
import alluxio.hub.proto.SetPrestoConfDirResponse;
import alluxio.hub.proto.SpeedStat;
import alluxio.hub.proto.SpeedTestParameter;
import alluxio.hub.proto.SpeedTestRequest;
import alluxio.hub.proto.SpeedTestResponse;
import alluxio.hub.proto.UploadFile;
import alluxio.hub.proto.UploadProcessType;
import alluxio.hub.proto.ValidationResult;
import alluxio.hub.proto.ValidationStatus;
import alluxio.hub.test.BaseHubTest;
import alluxio.stress.cli.UfsIOBench;
import alluxio.stress.worker.IOTaskSummary;
import alluxio.wire.MountPointInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ManagerProcessContextTest extends BaseHubTest {

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  private ManagerProcessContext mContext;
  private HubCluster mMockHub;
  private AlluxioCluster mMockAlluxio;
  private ScheduledExecutorService mMockExec;
  private AlluxioConfiguration mConf;

  @Before
  public void before() throws Exception {
    mConf = getTestConfig();
    mMockHub = mock(HubCluster.class);
    mMockAlluxio = mock(AlluxioCluster.class);
    mMockExec = mock(ScheduledExecutorService.class);
    mContext =
        Mockito.spy(new ManagerProcessContext(mConf, mMockExec, mMockAlluxio, mMockHub));
  }

  @After
  public void after() throws IOException {
    mContext.close();
  }

  @Test
  public void testClusterId() throws Exception {
    assertTrue(ManagerProcessContext.checkClusterId("abcd"));
    assertTrue(ManagerProcessContext.checkClusterId("ab12"));
    assertTrue(ManagerProcessContext.checkClusterId("1234"));
    assertFalse(ManagerProcessContext.checkClusterId("ABCD"));
    assertFalse(ManagerProcessContext.checkClusterId("abc!"));
    assertFalse(ManagerProcessContext.checkClusterId("123A"));
    assertFalse(ManagerProcessContext.checkClusterId("abcde"));
    assertFalse(ManagerProcessContext.checkClusterId("!@#$"));
    InstancedConfiguration conf = getTestConfig();
    getTestManagerContext(conf);
    // invalid cluster id throws exception
    conf.set(PropertyKey.HUB_CLUSTER_ID, "abcde");
    assertThrows(RuntimeException.class, () -> getTestManagerContext(conf));
  }

  @Test
  public void testGetAlluxioCluster() throws Exception {
    assertNotNull(getTestManagerContext().getAlluxioCluster());
  }

  @Test
  public void testGetHubManagerCluster() throws Exception {
    assertNotNull(getTestManagerContext().getHubCluster());
  }

  @Test
  public void testExecOnHubEmpty() {
    Set<HubNodeAddress> nodes = Collections.singleton(HubNodeAddress
        .newBuilder().setHostname(UUID.randomUUID().toString()).build());
    doReturn(nodes)
        .when(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
    AtomicInteger i = new AtomicInteger(0);
    Function<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub, Integer> action =
        client -> i.get();
    mContext.execOnHub(action, AlluxioNodeType.MASTER);
    verify(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
    verify(mMockHub).exec(nodes, mConf, action, mMockExec);
  }

  @Test
  public void testConfigurationSetFor() {
    Set<HubNodeAddress> nodes = Collections.singleton(HubNodeAddress
        .newBuilder().setHostname(UUID.randomUUID().toString()).build());
    doReturn(nodes)
        .when(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
    mContext.configurationSetFor(AlluxioNodeType.MASTER);
    verify(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
    verify(mMockHub).exec(eq(nodes), eq(mConf), any(), eq(mMockExec));
  }

  @Test
  public void testEmptyConfigurationSetFor() {
    doReturn(Collections.emptySet())
        .when(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
    assertThrows(IllegalStateException.class, () -> {
      mContext.configurationSetFor(AlluxioNodeType.MASTER);
    });
    verify(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
  }

  @Test
  public void testUpdateConfiguration() {
    Set<HubNodeAddress> nodes = Collections.singleton(HubNodeAddress
        .newBuilder().setHostname(UUID.randomUUID().toString()).build());
    doReturn(nodes)
        .when(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
    mContext.updateConfigurationFor(AlluxioNodeType.MASTER, mock(AlluxioConfigurationSet.class));
    verify(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
    verify(mMockHub).exec(eq(nodes), eq(mConf), any(), eq(mMockExec));
  }

  @Test
  public void testListCatalogs() throws IOException {
    Set<String> nodes = Collections.singleton(UUID.randomUUID().toString());
    doReturn(nodes)
        .when(mMockHub).nodesFromAlluxio(mMockAlluxio, AlluxioNodeType.MASTER);
    Path prestoCatalogDir =
        Paths.get(mTemp.getRoot().getAbsolutePath(), "etc/presto/conf/catalog");
    Files.createDirectories(prestoCatalogDir);
    PrestoCatalogListing r = PrestoCatalogListing.newBuilder()
        .setCatalogName("hive-test")
        .setHiveMetastoreUri("thrift://blahblahblah:9083").build();
    Map<HubNodeAddress, AgentListCatalogResponse> map = new HashMap<>();
    map.put(HubNodeAddress.newBuilder().build(),
        AgentListCatalogResponse.newBuilder().addCatalog(r).build());
    doReturn(map).when(mMockHub).exec(any(), any(), any(), any());
    assertEquals(PrestoCatalogListingResult.newBuilder().addCatalog(r).build(),
        mContext.listCatalogs(prestoCatalogDir.toString()));
  }

  @Test
  public void testConvertValidationResult() {
    Map<ValidationUtils.State, List<ValidationTaskResult>> res = new HashMap<>();
    List<ValidationTaskResult> results = new ArrayList<>();
    results.add(new ValidationTaskResult().setName("test1").setState(ValidationUtils.State.FAILED)
        .setDesc("aaaa").setOutput("bbbb").setAdvice("adv1"));
    List<ValidationTaskResult> results2 = new ArrayList<>();
    results2.add(new ValidationTaskResult().setName("test2").setState(ValidationUtils.State.SKIPPED)
        .setDesc("cccc").setOutput("dddd").setOutput("out2").setAdvice("adv2"));
    res.put(ValidationUtils.State.FAILED, results);
    res.put(ValidationUtils.State.SKIPPED, results2);
    List<ValidationResult> conv = HubUtil.convertValidationResult(res);
    assertEquals(2, conv.size());
    assertEquals(1, conv.stream().filter(f -> f.getName().equals("test1")).count());
    assertEquals(1, conv.stream().filter(f -> f.getName().equals("test2")).count());
    ValidationResult t1 = conv.stream().filter(f -> f.getName().equals("test1")).findAny()
        .orElseThrow(() -> new RuntimeException("Didn't have test1 object"));
    assertEquals(ValidationStatus.FAILED, t1.getTestResult());
    assertEquals("adv1", t1.getAdvice());
    assertEquals("aaaa", t1.getDescription());
    assertEquals("bbbb", t1.getOutput());
  }

  @Test
  public void testListMountPoints() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    Map<String, MountPointInfo> mpi = new HashMap<>();
    mpi.put("/", new MountPointInfo()
        .setUfsUri("hdfs://localhost:9000/")
        .setReadOnly(true)
        .setUfsType("hdfs")
        .setUfsCapacityBytes(9001));
    doReturn(mockFs).when(mContext).getFileSystem();
    doReturn(mpi).when(mockFs).getMountTable();
    ListMountPointResponse.Payload mpl = mContext.getMountPointList();
    assertEquals(1, mpl.getMountPointList().size());
    alluxio.hub.proto.MountPointInfo i = mpl.getMountPointList().get(0);
    assertEquals("hdfs://localhost:9000/", i.getUfsUri());
    assertTrue(i.getReadOnly());
    assertEquals("/", i.getAlluxioPath());
    assertEquals(0, i.getPropertiesCount());
  }

  @Test
  public void testApplyNewMount() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    Map<String, MountPointInfo> mpi = new HashMap<>();
    mpi.put("/", new MountPointInfo()
        .setUfsUri("hdfs://localhost:9000/")
        .setReadOnly(true)
        .setUfsType("hdfs")
        .setUfsCapacityBytes(9001));
    doReturn(mockFs).when(mContext).getFileSystem();
    doReturn(mpi).when(mockFs).getMountTable();

    ApplyMountPointRequest vr = ApplyMountPointRequest.newBuilder()
        .setPayload(ApplyMountPointRequest.Payload.newBuilder()
          .setNew(mContext.getMountPointList().getMountPointList().get(0)))
        .build();
    mContext.applyMount(vr);
  }

  @Test
  public void testApplyExistingMount() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    Map<String, MountPointInfo> mpi = new HashMap<>();
    mpi.put("/", new MountPointInfo()
        .setUfsUri("hdfs://localhost:9000/")
        .setReadOnly(true)
        .setUfsType("hdfs")
        .setUfsCapacityBytes(9001));
    doReturn(mockFs).when(mContext).getFileSystem();
    doReturn(mpi).when(mockFs).getMountTable();

    ApplyMountPointRequest vr = ApplyMountPointRequest.newBuilder()
        .setPayload(ApplyMountPointRequest.Payload.newBuilder()
          .setNew(mContext.getMountPointList().getMountPointList().get(0))
          .setBefore(mContext.getMountPointList().getMountPointList().get(0)))
        .build();
    mContext.applyMount(vr);
  }

  @Test
  public void testApplyRootMountToOtherPath() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    Map<String, MountPointInfo> mpi = new HashMap<>();
    mpi.put("/", new MountPointInfo().setUfsUri("hdfs://localhost:9000/").setReadOnly(true)
        .setUfsType("hdfs").setUfsCapacityBytes(9001));
    doReturn(mockFs).when(mContext).getFileSystem();
    doReturn(mpi).when(mockFs).getMountTable();

    ApplyMountPointRequest vr = ApplyMountPointRequest.newBuilder()
        .setPayload(ApplyMountPointRequest.Payload.newBuilder()
          .setNew(mContext.getMountPointList().getMountPointList().get(0)
              .toBuilder().setAlluxioPath("/mnt/new"))
          .setBefore(mContext.getMountPointList().getMountPointList().get(0)))
        .build();
    ApplyMountPointResponse.Payload resp = mContext.applyMount(vr);
    assertFalse(resp.getSuccess());
    assertTrue(resp.getErrorCount() > 0);
  }

  @Test
  public void testListFiles() {
    AgentListFileResponse r = AgentListFileResponse.newBuilder()
        .addFileInfo(AgentListFileInfo.newBuilder()
            .setFileName("abc123")
            .setProcessType(UploadProcessType.PRESTO)
            .build())
        .build();
    HubNodeAddress addr = HubTestUtils.generateNodeAddress();
    Map<HubNodeAddress, Pair<AlluxioNodeType, AgentListFileResponse>> resp = new HashMap<>();
    resp.put(addr, new Pair<>(AlluxioNodeType.MASTER, r));
    doReturn(resp).when(mContext).execOnHub(any(), eq(AlluxioNodeType.MASTER), any());
    doReturn(new HashMap<>()).when(mContext).execOnHub(any(), eq(AlluxioNodeType.WORKER), any());
    List<ListFile> s = mContext.listFiles();
    assertNotNull(s);
    assertEquals(1, s.size());
    assertTrue(s.get(0).hasLocation());
    assertTrue(s.get(0).hasName());
    assertTrue(s.get(0).hasProcessType());
    assertEquals(AlluxioNodeType.MASTER, s.get(0).getLocation());
    assertEquals("abc123", s.get(0).getName());
    assertEquals(UploadProcessType.PRESTO, s.get(0).getProcessType());
  }

  @Test
  public void testAddFile() {
    AgentFileUploadResponse r = AgentFileUploadResponse.newBuilder()
        .setSuccess(true)
        .setVersion(2)
        .setFileName("uptime")
        .build();
    HubNodeAddress addr = HubTestUtils.generateNodeAddress();
    Map<HubNodeAddress, AgentFileUploadResponse> resp = new HashMap<>();
    resp.put(addr, r);
    UploadFile req = UploadFile.newBuilder()
        .setDestination(AlluxioNodeType.WORKER)
        .setProcessType(UploadProcessType.PRESTO)
        .setContent("#!/usr/bin/env bash\nuptime")
        .setName("uptime")
        .setPermission("0777")
        .build();
    doReturn(resp).when(mContext).execOnHub(any(), any(), any());
    boolean addFileResp = mContext.uploadFile(Collections.singletonList(req));
    assertTrue(addFileResp);
    resp.put(HubTestUtils.generateNodeAddress(), r.toBuilder().setSuccess(false).build());
    assertFalse(mContext.uploadFile(Collections.singletonList(req)));
  }

  @Test
  public void testRemoveFile() {
    AgentRemoveFileResponse r = AgentRemoveFileResponse.newBuilder()
        .setSuccess(true)
        .build();
    HubNodeAddress addr = HubTestUtils.generateNodeAddress();
    Map<HubNodeAddress, AgentRemoveFileResponse> resp = new HashMap<>();
    resp.put(addr, r);
    RemoveFile req = RemoveFile.newBuilder()
        .setLocation(AlluxioNodeType.WORKER)
        .setProcessType(UploadProcessType.PRESTO)
        .setName("uptime")
        .build();
    doReturn(resp).when(mContext).execOnHub(any(), any(), any());
    boolean addFileResp = mContext.removeFile(Collections.singletonList(req));
    assertTrue(addFileResp);
    resp.put(HubTestUtils.generateNodeAddress(), r.toBuilder().setSuccess(false).build());
    assertFalse(mContext.removeFile(Collections.singletonList(req)));
  }

  @Test
  public void testSpeedTest() throws Exception {
    UfsIOBench bench = Mockito.mock(UfsIOBench.class);
    FileSystem mockFs = Mockito.mock(FileSystem.class);
    doReturn(mockFs).when(mContext).getFileSystem();
    doReturn(bench).when(mContext).getIOBench();
    Map<String, MountPointInfo> mt = new HashMap<>();
    MountPointInfo info = new MountPointInfo()
        .setMountId(1)
        .setReadOnly(true)
        .setUfsType("hdfs")
        .setUfsUri("hdfs://localhost:9000");
    mt.put("/", info);
    doReturn(mt).when(mockFs).getMountTable();
    IOTaskSummary summ = new IOTaskSummary();
    IOTaskSummary.SpeedStat read = generateSpeedStat();
    IOTaskSummary.SpeedStat write = generateSpeedStat();
    summ.setReadSpeedStat(read);
    summ.setWriteSpeedStat(write);
    doReturn(summ.toJson()).when(bench).run(any());
    SpeedTestResponse.Payload resp = mContext.speedTest(SpeedTestRequest.newBuilder()
            .setPayload(SpeedTestRequest.Payload.newBuilder()
                    .setMountPoint("/")
                    .setPath("bbb")
                    .setClusterParallelism(0)
                    .setNodeParallelism(0)
                    .setDataSize("1m"))
            .build());
    SpeedStat readResp = resp.getReadSpeedStat();
    SpeedStat writeResp = resp.getWriteSpeedStat();
    List<String> errResp = resp.getErrorList();
    SpeedTestParameter params = resp.getParameters();
    assertEquals(read.mAvgSpeedMbps, readResp.getAvgSpeedMbps(), 1E-10);
    assertEquals(read.mMinSpeedMbps, readResp.getMinSpeedMbps(), 1E-10);
    assertEquals(read.mMaxSpeedMbps, readResp.getMaxSpeedMbps(), 1E-10);
    assertEquals(read.mStdDev, readResp.getStdDev(), 1E-10);
    assertEquals(read.mTotalDurationSeconds, readResp.getTotalDurationSeconds(), 1E-10);
    assertEquals(read.mTotalSizeBytes, readResp.getTotalSizeBytes(), 1E-10);
    assertEquals(write.mAvgSpeedMbps, writeResp.getAvgSpeedMbps(), 1E-10);
    assertEquals(write.mMinSpeedMbps, writeResp.getMinSpeedMbps(), 1E-10);
    assertEquals(write.mMaxSpeedMbps, writeResp.getMaxSpeedMbps(), 1E-10);
    assertEquals(write.mStdDev, writeResp.getStdDev(), 1E-10);
    assertEquals(write.mTotalDurationSeconds, writeResp.getTotalDurationSeconds(), 1E-10);
    assertEquals(write.mTotalSizeBytes, writeResp.getTotalSizeBytes(), 1E-10);
    assertEquals(0, errResp.size());
    summ.setErrors(Collections.singletonList("testErr"));
    doReturn(summ.toJson()).when(bench).run(any());
    resp = mContext.speedTest(SpeedTestRequest.newBuilder()
            .setPayload(SpeedTestRequest.Payload.newBuilder()
                    .setMountPoint("/")
                    .setPath("bbb")
                    .setClusterParallelism(0)
                    .setNodeParallelism(0)
                    .setDataSize("1m"))
            .build());
    errResp = resp.getErrorList();
    assertEquals(1, errResp.size());
    assertEquals("testErr", errResp.get(0));
  }

  private static IOTaskSummary.SpeedStat generateSpeedStat() {
    IOTaskSummary.SpeedStat stat = new IOTaskSummary.SpeedStat();
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    stat.mAvgSpeedMbps = rng.nextDouble();
    stat.mMaxSpeedMbps = rng.nextDouble();
    stat.mMinSpeedMbps = rng.nextDouble();
    stat.mStdDev = rng.nextDouble();
    stat.mTotalDurationSeconds = rng.nextDouble();
    stat.mTotalSizeBytes = rng.nextLong();
    return stat;
  }

  @Test
  public void setPrestoConfDir() {
    Map<HubNodeAddress, AgentSetPrestoConfResponse> map = new HashMap<>();
    map.put(HubTestUtils.generateNodeAddress(),
            AgentSetPrestoConfResponse.newBuilder().setSuccess(true).build());
    doReturn(map).when(mContext).execOnHub(any(), any(), any());
    SetPrestoConfDirResponse.Payload resp =  mContext.setPrestoConfDir(
        SetPrestoConfDirRequest.newBuilder()
                .setPayload(SetPrestoConfDirRequest.Payload.newBuilder()
                        .setConfDir("/etc/presto"))
                .build());
    assertTrue(resp.getSuccess());
    assertFalse(resp.getIsDefault());

    map = new HashMap<>();
    map.put(HubTestUtils.generateNodeAddress(),
            AgentSetPrestoConfResponse.newBuilder().setSuccess(true).build());
    doReturn(map).when(mContext).execOnHub(any(), any(), any());
    resp = mContext.setPrestoConfDir(
            SetPrestoConfDirRequest.newBuilder()
                    .setPayload(SetPrestoConfDirRequest.Payload.newBuilder()
                            .setConfDir((String) PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH
                                .getDefaultValue()))
                    .build());
    assertTrue(resp.getSuccess());
    assertTrue(resp.getIsDefault());

    map = new HashMap<>();
    doReturn(AlluxioConfigurationSet.newBuilder()
        .setAlluxioEnv("aaa").setSiteProperties("bbb").setLog4JProperties("ccc").build())
        .when(mContext).configurationSetFor(any());
    map.put(HubTestUtils.generateNodeAddress(),
            AgentSetPrestoConfResponse.newBuilder().setSuccess(false).build());
    doReturn(map).when(mContext).execOnHub(any(), any(), any());
    resp = mContext.setPrestoConfDir(
            SetPrestoConfDirRequest.newBuilder()
                    .setPayload(SetPrestoConfDirRequest.Payload.newBuilder()
                            .setConfDir((String) PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH
                                .getDefaultValue()))
                    .build());
    assertFalse(resp.getSuccess());
    assertTrue(resp.getIsDefault());

    map = new HashMap<>();
    map.put(HubTestUtils.generateNodeAddress(),
            AgentSetPrestoConfResponse.newBuilder().setSuccess(false).build());
    doReturn(map).when(mContext).execOnHub(any(), any(), any());
    resp = mContext.setPrestoConfDir(
            SetPrestoConfDirRequest.newBuilder()
                    .setPayload(SetPrestoConfDirRequest.Payload.newBuilder()
                            .setConfDir((String) PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH
                                .getDefaultValue()))
                    .build());
    assertFalse(resp.getSuccess());
  }
}
