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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

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
import alluxio.wire.MasterWebUIConfiguration;
import alluxio.wire.MasterWebUIData;
import alluxio.wire.MasterWebUIInit;
import alluxio.wire.MasterWebUILogs;
import alluxio.wire.MasterWebUIMetrics;
import alluxio.wire.MasterWebUIWorkers;
import alluxio.wire.MountPointInfo;
import alluxio.wire.WorkerInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
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
    assertEquals(total, capacity.getTotal());
    assertEquals(0, capacity.getUsed());
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
    assertEquals(expectedValue, getInfo(params).getConfiguration().get(key.toString()));
  }

  @Test
  public void getLostWorkers() throws Exception {
    List<WorkerInfo> lostWorkersInfo = getInfo(NO_PARAMS).getLostWorkers();
    assertEquals(0, lostWorkersInfo.size());
  }

  @Test
  public void getMetrics() throws Exception {
    assertEquals(Long.valueOf(0),
        getInfo(NO_PARAMS).getMetrics().get(MetricsSystem.getMetricName("CompleteFileOps")));
  }

  @Test
  public void getMountPoints() throws Exception {
    Map<String, MountPointInfo> mountTable = mFileSystemMaster.getMountTable();
    Map<String, MountPointInfo> mountPoints = getInfo(NO_PARAMS).getMountPoints();
    assertEquals(mountTable.size(), mountPoints.size());
    for (Map.Entry<String, MountPointInfo> mountPoint : mountTable.entrySet()) {
      assertTrue(mountPoints.containsKey(mountPoint.getKey()));
      String expectedUri = mountPoints.get(mountPoint.getKey()).getUfsUri();
      String returnedUri = mountPoint.getValue().getUfsUri();
      assertEquals(expectedUri, returnedUri);
    }
  }

  @Test
  public void getRpcAddress() throws Exception {
    assertTrue(getInfo(NO_PARAMS).getRpcAddress().contains(String.valueOf(
        NetworkAddressUtils.getPort(ServiceType.MASTER_RPC, ServerConfiguration.global()))));
  }

  @Test
  public void getStartTimeMs() throws Exception {
    assertTrue(getInfo(NO_PARAMS).getStartTimeMs() > 0);
  }

  @Test
  public void getStartupConsistencyCheckStatus() throws Exception {
    MasterTestUtils.waitForStartupConsistencyCheck(mFileSystemMaster);
    alluxio.wire.StartupConsistencyCheck status = getInfo(NO_PARAMS).getStartupConsistencyCheck();
    assertEquals(StartupConsistencyCheck.Status.COMPLETE.toString().toLowerCase(),
        status.getStatus());
    assertEquals(0, status.getInconsistentUris().size());
  }

  @Test
  public void getTierCapacity() throws Exception {
    long total = ServerConfiguration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    Capacity capacity = getInfo(NO_PARAMS).getTierCapacity().get("MEM");
    assertEquals(total, capacity.getTotal());
    assertEquals(0, capacity.getUsed());
  }

  @Test
  public void getUptimeMs() throws Exception {
    assertTrue(getInfo(NO_PARAMS).getUptimeMs() > 0);
  }

  @Test
  public void getUfsCapacity() throws Exception {
    Capacity ufsCapacity = getInfo(NO_PARAMS).getUfsCapacity();
    if (UnderFileSystemTestUtils.isObjectStorage(mFileSystemMaster.getUfsAddress())) {
      // Object storage ufs capacity is always invalid.
      assertEquals(-1, ufsCapacity.getTotal());
    } else {
      assertTrue(ufsCapacity.getTotal() > 0);
    }
  }

  @Test
  public void getWorkers() throws Exception {
    List<WorkerInfo> workerInfos = getInfo(NO_PARAMS).getWorkers();
    assertEquals(1, workerInfos.size());
    WorkerInfo workerInfo = workerInfos.get(0);
    assertEquals(0, workerInfo.getUsedBytes());
    long bytes = ServerConfiguration.getBytes(PropertyKey.WORKER_MEMORY_SIZE);
    assertEquals(bytes, workerInfo.getCapacityBytes());
  }

  @Test
  public void getVersion() throws Exception {
    assertEquals(RuntimeConstants.VERSION, getInfo(NO_PARAMS).getVersion());
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
    assertEquals(expectedString, resultString);
  }

  private MasterWebUIConfiguration getWebUIConfigurationData() throws Exception {
    String result =
        new TestCase(mHostname, mPort, getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_CONFIG),
            NO_PARAMS, HttpMethod.GET, null).call();
    MasterWebUIConfiguration data =
        new ObjectMapper().readValue(result, MasterWebUIConfiguration.class);
    return data;
  }

  @Test
  public void WebUIConfiguration() throws Exception {
    MasterWebUIConfiguration result = getWebUIConfigurationData();
    String expectedJson =
        ("{`configuration`:[{`left`:`alluxio.conf.dir`,`middle`:`${alluxio.home}/conf`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.conf.validation.enabled`,`middle`:`true`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.debug`,`middle`:`false`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.extensions.dir`,`middle`:`${alluxio.home}/extensions`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.fuse.cached.paths.max`,`middle`:`500`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.fuse.debug.enabled`,`middle`:`false`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.fuse.fs.name`,`middle`:`alluxio-fuse`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.fuse.maxwrite.bytes`,`middle`:`128KB`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.fuse.user.group.translation.enabled`,"
            + "`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.home`,"
            + "`middle`:`/opt/alluxio`,`right`:`DEFAULT`},{`left`:`alluxio.integration.master"
            + ".resource.cpu`,`middle`:`1`,`right`:`DEFAULT`},{`left`:`alluxio.integration.master"
            + ".resource.mem`,`middle`:`1024MB`,`right`:`DEFAULT`},{`left`:`alluxio.integration"
            + ".mesos.alluxio.jar.url`,`middle`:`http://downloads.alluxio"
            + ".org/downloads/files/${alluxio.version}/alluxio-${alluxio.version}-bin.tar.gz`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.integration.mesos.jdk.path`,`middle`:`jdk1.8"
            + ".0_151`,`right`:`DEFAULT`},{`left`:`alluxio.integration.mesos.jdk.url`,"
            + "`middle`:`LOCAL`,`right`:`DEFAULT`},{`left`:`alluxio.integration.mesos.master"
            + ".name`,`middle`:`AlluxioMaster`,`right`:`DEFAULT`},{`left`:`alluxio.integration"
            + ".mesos.master.node.count`,`middle`:`1`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".integration.mesos.principal`,`middle`:`alluxio`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.integration.mesos.role`,`middle`:`*`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.integration.mesos.secret`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.integration.mesos.user`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.integration.mesos.worker.name`,`middle`:`AlluxioWorker`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.integration.worker.resource.cpu`,`middle`:`1`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.integration.worker.resource.mem`,"
            + "`middle`:`1024MB`,`right`:`DEFAULT`},{`left`:`alluxio.integration.yarn.workers.per"
            + ".host.max`,`middle`:`1`,`right`:`DEFAULT`},{`left`:`alluxio.job.master.bind.host`,"
            + "`middle`:`0.0.0.0`,`right`:`DEFAULT`},{`left`:`alluxio.job.master.client.threads`,"
            + "`middle`:`1024`,`right`:`DEFAULT`},{`left`:`alluxio.job.master.embedded.journal"
            + ".addresses`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.job.master.embedded"
            + ".journal.port`,`middle`:`20003`,`right`:`DEFAULT`},{`left`:`alluxio.job.master"
            + ".finished.job.retention.ms`,`middle`:`300000`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".job.master.hostname`,`middle`:`${alluxio.master.hostname}`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.job.master.job.capacity`,`middle`:`100000`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.job.master.lost.worker.interval.ms`,`middle`:`1000`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.job.master.rpc.addresses`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.job.master.rpc.port`,`middle`:`60666`,"
            + "`right`:`RUNTIME`},{`left`:`alluxio.job.master.web.bind.host`,`middle`:`0.0.0.0`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.job.master.web.hostname`,`middle`:`${alluxio"
            + ".job.master.hostname}`,`right`:`DEFAULT`},{`left`:`alluxio.job.master.web.port`,"
            + "`middle`:`60667`,`right`:`RUNTIME`},{`left`:`alluxio.job.master.worker.heartbeat"
            + ".interval.ms`,`middle`:`1000`,`right`:`DEFAULT`},{`left`:`alluxio.job.master"
            + ".worker.timeout.ms`,`middle`:`60000`,`right`:`DEFAULT`},{`left`:`alluxio.job"
            + ".worker.bind.host`,`middle`:`0.0.0.0`,`right`:`DEFAULT`},{`left`:`alluxio.job"
            + ".worker.data.port`,`middle`:`30002`,`right`:`DEFAULT`},{`left`:`alluxio.job.worker"
            + ".hostname`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.job.worker.rpc.port`,"
            + "`middle`:`30001`,`right`:`DEFAULT`},{`left`:`alluxio.job.worker.web.bind.host`,"
            + "`middle`:`0.0.0.0`,`right`:`DEFAULT`},{`left`:`alluxio.job.worker.web.port`,"
            + "`middle`:`30003`,`right`:`DEFAULT`},{`left`:`alluxio.jvm.monitor.info.threshold`,"
            + "`middle`:`1sec`,`right`:`DEFAULT`},{`left`:`alluxio.jvm.monitor.sleep.interval`,"
            + "`middle`:`1sec`,`right`:`DEFAULT`},{`left`:`alluxio.jvm.monitor.warn.threshold`,"
            + "`middle`:`10sec`,`right`:`DEFAULT`},{`left`:`alluxio.locality.compare.node.ip`,"
            + "`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.locality.node`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.locality.order`,`middle`:`node,rack`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.locality.rack`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.locality.script`,`middle`:`alluxio-locality.sh`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.logger.type`,`middle`:`Console`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.logs.dir`,`middle`:`${alluxio.work.dir}/logs`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.logserver.hostname`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.logserver.logs.dir`,`middle`:`${alluxio.work"
            + ".dir}/logs`,`right`:`DEFAULT`},{`left`:`alluxio.logserver.port`,`middle`:`45600`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.logserver.threads.max`,`middle`:`2048`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.logserver.threads.min`,`middle`:`512`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.activesync.batchinterval`,"
            + "`middle`:`1sec`,`right`:`DEFAULT`},{`left`:`alluxio.master.activesync.eventrate"
            + ".interval`,`middle`:`60sec`,`right`:`DEFAULT`},{`left`:`alluxio.master.activesync"
            + ".initialsync`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".activesync.interval`,`middle`:`30sec`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".activesync.maxactivity`,`middle`:`10`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".activesync.maxage`,`middle`:`10`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".activesync.polltimeout`,`middle`:`10sec`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".master.activesync.retry.timeout`,`middle`:`1hour`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.activesync.threadpoolsize`,`middle`:`3`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.audit.logging.enabled`,"
            + "`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.master.audit.logging.queue"
            + ".capacity`,`middle`:`10000`,`right`:`DEFAULT`},{`left`:`alluxio.master.backup"
            + ".directory`,`middle`:`/alluxio_backups`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".bind.host`,`middle`:`10.238.141.254`,`right`:`RUNTIME`},{`left`:`alluxio.master"
            + ".client.socket.cleanup.interval`,`middle`:`10min`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.cluster.metrics.update.interval`,`middle`:`1m`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.connection.timeout`,`middle`:`0`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.daily.backup.enabled`,`middle`:`false`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.daily.backup.files.retained`,"
            + "`middle`:`3`,`right`:`DEFAULT`},{`left`:`alluxio.master.daily.backup.time`,"
            + "`middle`:`05:00`,`right`:`DEFAULT`},{`left`:`alluxio.master.embedded.journal"
            + ".addresses`,`middle`:`localhost:${alluxio.master.embedded.journal.port}`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.embedded.journal.election.timeout`,"
            + "`middle`:`5s`,`right`:`DEFAULT`},{`left`:`alluxio.master.embedded.journal"
            + ".heartbeat.interval`,`middle`:`1s`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".embedded.journal.port`,`middle`:`19200`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".master.embedded.journal.storage.level`,`middle`:`DISK`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.file.async.persist.handler`,`middle`:`alluxio.master.file"
            + ".async.DefaultAsyncPersistHandler`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".format.file_prefix`,`middle`:`_format_`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".master.grpc.channel.auth.timeout`,`middle`:`2sec`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.master.grpc.channel.shutdown.timeout`,`middle`:`3sec`,"
            + "`right`:`RUNTIME`},{`left`:`alluxio.master.grpc.server.shutdown.timeout`,"
            + "`middle`:`3sec`,`right`:`RUNTIME`},{`left`:`alluxio.master.heartbeat.timeout`,"
            + "`middle`:`10min`,`right`:`DEFAULT`},{`left`:`alluxio.master.hostname`,`middle`:`10"
            + ".238.141.254`,`right`:`RUNTIME`},{`left`:`alluxio.master.journal.checkpoint.period"
            + ".entries`,`middle`:`2000000`,`right`:`DEFAULT`},{`left`:`alluxio.master.journal"
            + ".flush.batch.time`,`middle`:`5ms`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".journal.flush.timeout`,`middle`:`1sec`,`right`:`RUNTIME`},{`left`:`alluxio.master"
            + ".journal.folder`,`middle`:`/var/folders/zk/l642b_mn4097j57qt1wgk3mm0000gn/T"
            + "/alluxio-tests/test-cluster-f7fa676e-37f8-4ceb-bf7c-c07bb5e3465b/journal`,"
            + "`right`:`RUNTIME`},{`left`:`alluxio.master.journal.formatter.class`,"
            + "`middle`:`alluxio.master.journalv0.ProtoBufJournalFormatter`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.journal.gc.period`,`middle`:`2min`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.journal.gc.threshold`,`middle`:`5min`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.journal.init.from.backup`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.journal.log.size.bytes.max`,`middle`:`10MB`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.journal.retry.interval`,"
            + "`middle`:`1sec`,`right`:`DEFAULT`},{`left`:`alluxio.master.journal.tailer.shutdown"
            + ".quiet.wait.time`,`middle`:`50ms`,`right`:`RUNTIME`},{`left`:`alluxio.master"
            + ".journal.tailer.sleep.time`,`middle`:`10ms`,`right`:`RUNTIME`},{`left`:`alluxio"
            + ".master.journal.temporary.file.gc.threshold`,`middle`:`30min`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.journal.type`,`middle`:`UFS`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.journal.ufs.option`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.jvm.monitor.enabled`,`middle`:`false`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.keytab.file`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.lockcache.concurrency.level`,`middle`:`100`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.lockcache.initsize`,`middle`:`1000`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.lockcache.maxsize`,`middle`:`100000`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.log.config.report.heartbeat.interval`,"
            + "`middle`:`1h`,`right`:`DEFAULT`},{`left`:`alluxio.master.master.heartbeat"
            + ".interval`,`middle`:`2min`,`right`:`DEFAULT`},{`left`:`alluxio.master.metastore`,"
            + "`middle`:`HEAP`,`right`:`DEFAULT`},{`left`:`alluxio.master.metastore.dir`,"
            + "`middle`:`/var/folders/zk/l642b_mn4097j57qt1wgk3mm0000gn/T/alluxio-tests/test"
            + "-cluster-f7fa676e-37f8-4ceb-bf7c-c07bb5e3465b/metastore`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.master.metastore.inode.cache.evict.batch.size`,`middle`:`1000`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.metastore.inode.cache.high.water.mark"
            + ".ratio`,`middle`:`0.85`,`right`:`DEFAULT`},{`left`:`alluxio.master.metastore.inode"
            + ".cache.low.water.mark.ratio`,`middle`:`0.7`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".master.metastore.inode.cache.max.size`,`middle`:`10000000`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.mount.table.root.alluxio`,`middle`:`/`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.mount.table.root.option`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.mount.table.root.readonly`,`middle`:`false`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.mount.table.root.shared`,"
            + "`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.master.mount.table.root.ufs`,"
            + "`middle`:`${alluxio.underfs.address}`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".periodic.block.integrity.check.interval`,`middle`:`1hr`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.periodic.block.integrity.check.repair`,`middle`:`false`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.persistence.checker.interval.ms`,"
            + "`middle`:`1000`,`right`:`DEFAULT`},{`left`:`alluxio.master.persistence.initial"
            + ".interval.ms`,`middle`:`1000`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".persistence.initial.wait.time.ms`,`middle`:`0`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.persistence.max.interval.ms`,`middle`:`3600000`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.persistence.max.total.wait.time.ms`,"
            + "`middle`:`86400000`,`right`:`DEFAULT`},{`left`:`alluxio.master.persistence"
            + ".scheduler.interval.ms`,`middle`:`1000`,`right`:`DEFAULT`},{`left`:`alluxio.master"
            + ".port`,`middle`:`60660`,`right`:`RUNTIME`},{`left`:`alluxio.master.principal`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.master.replication.check.interval"
            + ".ms`,`middle`:`60000`,`right`:`DEFAULT`},{`left`:`alluxio.master.rpc.addresses`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.master.serving.thread.timeout`,"
            + "`middle`:`5m`,`right`:`DEFAULT`},{`left`:`alluxio.master.startup.block.integrity"
            + ".check.enabled`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.master.startup"
            + ".consistency.check.enabled`,`middle`:`true`,`right`:`RUNTIME`},{`left`:`alluxio"
            + ".master.tieredstore.global.level0.alias`,`middle`:`MEM`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.tieredstore.global.level1.alias`,`middle`:`SSD`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.tieredstore.global.level2.alias`,"
            + "`middle`:`HDD`,`right`:`DEFAULT`},{`left`:`alluxio.master.tieredstore.global"
            + ".levels`,`middle`:`3`,`right`:`DEFAULT`},{`left`:`alluxio.master.ttl.checker"
            + ".interval`,`middle`:`1sec`,`right`:`RUNTIME`},{`left`:`alluxio.master.ufs.block"
            + ".location.cache.capacity`,`middle`:`1000000`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".master.ufs.path.cache.capacity`,`middle`:`100000`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.ufs.path.cache.threads`,`middle`:`64`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.master.web.bind.host`,`middle`:`10.238.141.254`,"
            + "`right`:`RUNTIME`},{`left`:`alluxio.master.web.hostname`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.master.web.port`,`middle`:`60665`,"
            + "`right`:`RUNTIME`},{`left`:`alluxio.master.worker.connect.wait.time`,"
            + "`middle`:`0sec`,`right`:`RUNTIME`},{`left`:`alluxio.master.worker.heartbeat"
            + ".interval`,`middle`:`10sec`,`right`:`DEFAULT`},{`left`:`alluxio.master.worker"
            + ".threads.max`,`middle`:`100`,`right`:`RUNTIME`},{`left`:`alluxio.master.worker"
            + ".threads.min`,`middle`:`1`,`right`:`RUNTIME`},{`left`:`alluxio.master.worker"
            + ".timeout`,`middle`:`5min`,`right`:`DEFAULT`},{`left`:`alluxio.metrics.conf.file`,"
            + "`middle`:`${alluxio.conf.dir}/metrics.properties`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.metrics.context.shutdown.timeout`,`middle`:`1sec`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.network.channel.health.check.timeout.ms`,"
            + "`middle`:`5sec`,`right`:`DEFAULT`},{`left`:`alluxio.network.host.resolution"
            + ".timeout`,`middle`:`250ms`,`right`:`RUNTIME`},{`left`:`alluxio.proxy.s3"
            + ".deletetype`,`middle`:`ALLUXIO_AND_UFS`,`right`:`DEFAULT`},{`left`:`alluxio.proxy"
            + ".s3.multipart.temporary.dir.suffix`,`middle`:`_s3_multipart_tmp`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.proxy.s3.writetype`,`middle`:`CACHE_THROUGH`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.proxy.stream.cache.timeout`,`middle`:`1hour`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.proxy.web.bind.host`,`middle`:`0.0.0.0`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.proxy.web.hostname`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.proxy.web.port`,`middle`:`60685`,"
            + "`right`:`RUNTIME`},{`left`:`alluxio.security.authentication.custom.provider"
            + ".class`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.security.authentication"
            + ".type`,`middle`:`NOSASL`,`right`:`RUNTIME`},{`left`:`alluxio.security"
            + ".authorization.permission.enabled`,`middle`:`false`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.security.authorization.permission.supergroup`,"
            + "`middle`:`supergroup`,`right`:`DEFAULT`},{`left`:`alluxio.security.authorization"
            + ".permission.umask`,`middle`:`022`,`right`:`DEFAULT`},{`left`:`alluxio.security"
            + ".group.mapping.cache.timeout`,`middle`:`1min`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".security.group.mapping.class`,`middle`:`alluxio.security.group.provider"
            + ".ShellBasedUnixGroupsMapping`,`right`:`DEFAULT`},{`left`:`alluxio.security.login"
            + ".impersonation.username`,`middle`:`_HDFS_USER_`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.security.login.username`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.security.stale.channel.purge.interval`,`middle`:`60min`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.site.conf.dir`,`middle`:`${alluxio.conf.dir}/,"
            + "/Users/William/.alluxio/,/etc/alluxio/`,`right`:`DEFAULT`},{`left`:`alluxio.test"
            + ".mode`,`middle`:`true`,`right`:`RUNTIME`},{`left`:`alluxio.tmp.dirs`,"
            + "`middle`:`/tmp`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.address`,"
            + "`middle`:`${alluxio.work.dir}/underFSStorage`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".underfs.allow.set.owner.failure`,`middle`:`false`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.cleanup.enabled`,`middle`:`false`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.cleanup.interval`,`middle`:`1day`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.gcs.directory.suffix`,`middle`:`/`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.gcs.owner.id.to.username.mapping`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.hdfs.configuration`,"
            + "`middle`:`${alluxio.conf.dir}/core-site.xml:${alluxio.conf.dir}/hdfs-site.xml`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.hdfs.impl`,`middle`:`org.apache.hadoop"
            + ".hdfs.DistributedFileSystem`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.hdfs"
            + ".prefixes`,`middle`:`hdfs://,glusterfs:///,maprfs:///`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.hdfs.remote`,`middle`:`false`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.kodo.connect.timeout`,`middle`:`50sec`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.kodo.downloadhost`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.kodo.endpoint`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.kodo.requests.max`,`middle`:`64`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.listing.length`,`middle`:`1000`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.object.store.mount.shared.publicly`,"
            + "`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.object.store.multi"
            + ".range.chunk.size`,`middle`:`${alluxio.user.block.size.bytes.default}`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.object.store.read.retry.base.sleep`,"
            + "`middle`:`50ms`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.object.store.read"
            + ".retry.max.num`,`middle`:`20`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.object"
            + ".store.read.retry.max.sleep`,`middle`:`30sec`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".underfs.object.store.service.threads`,`middle`:`20`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.oss.connection.max`,`middle`:`1024`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.oss.connection.timeout`,`middle`:`50sec`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.oss.connection.ttl`,`middle`:`-1`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.oss.socket.timeout`,`middle`:`50sec`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3.admin.threads.max`,`middle`:`20`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3.disable.dns.buckets`,"
            + "`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3.endpoint`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3.owner.id.to.username"
            + ".mapping`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3.proxy.host`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3.proxy.port`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3.threads.max`,"
            + "`middle`:`40`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3.upload.threads.max`,"
            + "`middle`:`20`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3a.bulk.delete"
            + ".enabled`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3a"
            + ".consistency.timeout`,`middle`:`1min`,`right`:`DEFAULT`},{`left`:`alluxio.underfs"
            + ".s3a.directory.suffix`,`middle`:`/`,`right`:`DEFAULT`},{`left`:`alluxio.underfs"
            + ".s3a.inherit_acl`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3a"
            + ".intermediate.upload.clean.age`,`middle`:`3day`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.s3a.list.objects.v1`,`middle`:`false`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.s3a.request.timeout`,`middle`:`1min`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.s3a.secure.http.enabled`,`middle`:`false`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3a.server.side.encryption.enabled`,"
            + "`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3a.signer"
            + ".algorithm`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3a.socket"
            + ".timeout`,`middle`:`50sec`,`right`:`DEFAULT`},{`left`:`alluxio.underfs.s3a"
            + ".streaming.upload.enabled`,`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".underfs.s3a.streaming.upload.partition.size`,`middle`:`64MB`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.underfs.version`,`middle`:`2.2`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.app.id`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.user"
            + ".block.master.client.threads`,`middle`:`10`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".user.block.remote.read.buffer.size.bytes`,`middle`:`64`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.user.block.size.bytes.default`,`middle`:`1KB`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.user.block.worker.client.pool.gc.threshold`,`middle`:`300sec`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.block.worker.client.pool.size`,"
            + "`middle`:`1024`,`right`:`DEFAULT`},{`left`:`alluxio.user.block.worker.client.read"
            + ".retry`,`middle`:`5`,`right`:`DEFAULT`},{`left`:`alluxio.user.conf.cluster.default"
            + ".enabled`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.user.date.format"
            + ".pattern`,`middle`:`MM-dd-yyyy HH:mm:ss:SSS`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".user.failed.space.request.limits`,`middle`:`3`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.file.buffer.bytes`,`middle`:`8MB`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.file.cache.partially.read.block`,`middle`:`true`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.file.copyfromlocal.write.location.policy"
            + ".class`,`middle`:`alluxio.client.file.policy.RoundRobinPolicy`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.file.create.ttl`,`middle`:`-1`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.file.create.ttl.action`,`middle`:`DELETE`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.file.delete.unchecked`,`middle`:`false`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.file.load.ttl`,`middle`:`-1`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.file.load.ttl.action`,`middle`:`FREE`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.file.master.client.threads`,"
            + "`middle`:`10`,`right`:`DEFAULT`},{`left`:`alluxio.user.file.metadata.load.type`,"
            + "`middle`:`Once`,`right`:`DEFAULT`},{`left`:`alluxio.user.file.metadata.sync"
            + ".interval`,`middle`:`-1`,`right`:`DEFAULT`},{`left`:`alluxio.user.file.passive"
            + ".cache.enabled`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.user.file"
            + ".readtype.default`,`middle`:`CACHE_PROMOTE`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".user.file.replication.durable`,`middle`:`1`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".user.file.replication.max`,`middle`:`-1`,`right`:`DEFAULT`},{`left`:`alluxio.user"
            + ".file.replication.min`,`middle`:`0`,`right`:`DEFAULT`},{`left`:`alluxio.user.file"
            + ".seek.buffer.size.bytes`,`middle`:`1MB`,`right`:`DEFAULT`},{`left`:`alluxio.user"
            + ".file.ufs.tier.enabled`,`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.user"
            + ".file.waitcompleted.poll`,`middle`:`1sec`,`right`:`DEFAULT`},{`left`:`alluxio.user"
            + ".file.write.avoid.eviction.policy.reserved.size.bytes`,`middle`:`0MB`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.file.write.location.policy.class`,"
            + "`middle`:`alluxio.client.file.policy.LocalFirstPolicy`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.file.write.tier.default`,`middle`:`0`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.file.writetype.default`,`middle`:`MUST_CACHE`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.heartbeat.interval`,`middle`:`1sec`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.hostname`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.local.reader.chunk.size.bytes`,`middle`:`8MB`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.local.writer.chunk.size.bytes`,"
            + "`middle`:`64KB`,`right`:`DEFAULT`},{`left`:`alluxio.user.metrics.collection"
            + ".enabled`,`middle`:`false`,`right`:`RUNTIME`},{`left`:`alluxio.user.metrics"
            + ".heartbeat.interval`,`middle`:`3sec`,`right`:`DEFAULT`},{`left`:`alluxio.user"
            + ".network.data.timeout`,`middle`:`30sec`,`right`:`DEFAULT`},{`left`:`alluxio.user"
            + ".network.flowcontrol.window`,`middle`:`2MB`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".user.network.keepalive.time`,`middle`:`2147483647`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.network.keepalive.timeout`,`middle`:`30sec`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.network.max.inbound.message.size`,"
            + "`middle`:`4MB`,`right`:`DEFAULT`},{`left`:`alluxio.user.network.netty.channel`,"
            + "`middle`:`EPOLL`,`right`:`DEFAULT`},{`left`:`alluxio.user.network.netty.worker"
            + ".threads`,`middle`:`0`,`right`:`DEFAULT`},{`left`:`alluxio.user.network.reader"
            + ".buffer.size.messages`,`middle`:`16`,`right`:`DEFAULT`},{`left`:`alluxio.user"
            + ".network.reader.chunk.size.bytes`,`middle`:`64`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.user.network.socket.timeout`,`middle`:`10min`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.network.writer.buffer.size.messages`,`middle`:`16`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.network.writer.chunk.size.bytes`,"
            + "`middle`:`1MB`,`right`:`DEFAULT`},{`left`:`alluxio.user.network.writer.close"
            + ".timeout`,`middle`:`30min`,`right`:`DEFAULT`},{`left`:`alluxio.user.network.writer"
            + ".flush.timeout`,`middle`:`30min`,`right`:`DEFAULT`},{`left`:`alluxio.user.rpc"
            + ".retry.base.sleep`,`middle`:`50ms`,`right`:`DEFAULT`},{`left`:`alluxio.user.rpc"
            + ".retry.max.duration`,`middle`:`1s`,`right`:`RUNTIME`},{`left`:`alluxio.user.rpc"
            + ".retry.max.num.retry`,`middle`:`60`,`right`:`RUNTIME`},{`left`:`alluxio.user.rpc"
            + ".retry.max.sleep`,`middle`:`500ms`,`right`:`RUNTIME`},{`left`:`alluxio.user.short"
            + ".circuit.enabled`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.user.ufs"
            + ".block.location.all.fallback.enabled`,`middle`:`false`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.ufs.block.read.concurrency.max`,`middle`:`2147483647`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.user.ufs.block.read.location.policy`,"
            + "`middle`:`alluxio.client.file.policy.LocalFirstPolicy`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.ufs.block.read.location.policy.deterministic.hash.shards`,"
            + "`middle`:`1`,`right`:`DEFAULT`},{`left`:`alluxio.user.ufs.delegation.read.buffer"
            + ".size.bytes`,`middle`:`8MB`,`right`:`DEFAULT`},{`left`:`alluxio.user.ufs"
            + ".delegation.write.buffer.size.bytes`,`middle`:`2MB`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.user.worker.list.refresh.interval`,`middle`:`1s`,"
            + "`right`:`RUNTIME`},{`left`:`alluxio.version`,`middle`:`2.0.0-SNAPSHOT`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.web.file.info.enabled`,`middle`:`true`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.web.resources`,"
            + "`middle`:`/Users/William/git/alluxio/tests/../alluxio-ui`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.web.temp.path`,`middle`:`${alluxio.work.dir}/web/`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.web.threads`,`middle`:`1`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.webui.cors.enabled`,`middle`:`false`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.webui.refresh.interval.ms`,`middle`:`15000`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.work.dir`,"
            + "`middle`:`/var/folders/zk/l642b_mn4097j57qt1wgk3mm0000gn/T/alluxio-tests/test"
            + "-cluster-f7fa676e-37f8-4ceb-bf7c-c07bb5e3465b`,`right`:`RUNTIME`},{`left`:`alluxio"
            + ".worker.allocator.class`,`middle`:`alluxio.worker.block.allocator"
            + ".MaxFreeAllocator`,`right`:`DEFAULT`},{`left`:`alluxio.worker.bind.host`,"
            + "`middle`:`10.238.141.254`,`right`:`RUNTIME`},{`left`:`alluxio.worker.block"
            + ".heartbeat.interval`,`middle`:`15ms`,`right`:`RUNTIME`},{`left`:`alluxio.worker"
            + ".block.heartbeat.timeout`,`middle`:`${alluxio.worker.master.connect.retry"
            + ".timeout}`,`right`:`DEFAULT`},{`left`:`alluxio.worker.block.master.client.pool"
            + ".size`,`middle`:`11`,`right`:`DEFAULT`},{`left`:`alluxio.worker.block.threads"
            + ".max`,`middle`:`2048`,`right`:`RUNTIME`},{`left`:`alluxio.worker.block.threads"
            + ".min`,`middle`:`1`,`right`:`RUNTIME`},{`left`:`alluxio.worker.data.folder`,"
            + "`middle`:`/alluxioworker/`,`right`:`DEFAULT`},{`left`:`alluxio.worker.data.folder"
            + ".permissions`,`middle`:`rwxrwxrwx`,`right`:`DEFAULT`},{`left`:`alluxio.worker.data"
            + ".folder.tmp`,`middle`:`.tmp_blocks`,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".data.server.class`,`middle`:`alluxio.worker.grpc.GrpcDataServer`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.data.server.domain.socket.address`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker.data.server.domain.socket"
            + ".as.uuid`,`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.worker.data.tmp"
            + ".subdir.max`,`middle`:`1024`,`right`:`DEFAULT`},{`left`:`alluxio.worker.evictor"
            + ".class`,`middle`:`alluxio.worker.block.evictor.LRUEvictor`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.worker.evictor.lrfu.attenuation.factor`,`middle`:`2.0`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.evictor.lrfu.step.factor`,`middle`:`0"
            + ".25`,`right`:`DEFAULT`},{`left`:`alluxio.worker.file.buffer.size`,`middle`:`1MB`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.file.persist.pool.size`,`middle`:`64`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.file.persist.rate.limit`,"
            + "`middle`:`2GB`,`right`:`DEFAULT`},{`left`:`alluxio.worker.file.persist.rate.limit"
            + ".enabled`,`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.worker.filesystem"
            + ".heartbeat.interval`,`middle`:`1sec`,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".free.space.timeout`,`middle`:`10sec`,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".hostname`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker.jvm.monitor"
            + ".enabled`,`middle`:`false`,`right`:`DEFAULT`},{`left`:`alluxio.worker.keytab"
            + ".file`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker.master.connect.retry"
            + ".timeout`,`middle`:`1hour`,`right`:`DEFAULT`},{`left`:`alluxio.worker.memory"
            + ".size`,`middle`:`100MB`,`right`:`RUNTIME`},{`left`:`alluxio.worker.network.async"
            + ".cache.manager.threads.max`,`middle`:`8`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".worker.network.block.reader.threads.max`,`middle`:`2048`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.worker.network.flowcontrol.window`,`middle`:`2MB`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.network.keepalive.time`,"
            + "`middle`:`30sec`,`right`:`DEFAULT`},{`left`:`alluxio.worker.network.keepalive"
            + ".timeout`,`middle`:`10sec`,`right`:`DEFAULT`},{`left`:`alluxio.worker.network"
            + ".netty.boss.threads`,`middle`:`1`,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".network.netty.channel`,`middle`:`EPOLL`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".worker.network.netty.shutdown.quiet.period`,`middle`:`0ms`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.worker.network.netty.watermark.high`,`middle`:`32KB`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.network.netty.watermark.low`,"
            + "`middle`:`8KB`,`right`:`DEFAULT`},{`left`:`alluxio.worker.network.netty.worker"
            + ".threads`,`middle`:`2`,`right`:`RUNTIME`},{`left`:`alluxio.worker.network.reader"
            + ".max.chunk.size.bytes`,`middle`:`2MB`,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".network.shutdown.timeout`,`middle`:`0ms`,`right`:`RUNTIME`},{`left`:`alluxio"
            + ".worker.port`,`middle`:`0`,`right`:`RUNTIME`},{`left`:`alluxio.worker.principal`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker.session.timeout`,"
            + "`middle`:`1min`,`right`:`DEFAULT`},{`left`:`alluxio.worker.storage.checker"
            + ".enabled`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore"
            + ".block.lock.readers`,`middle`:`1000`,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".tieredstore.block.locks`,`middle`:`1000`,`right`:`DEFAULT`},{`left`:`alluxio"
            + ".worker.tieredstore.level0.alias`,`middle`:`MEM`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.worker.tieredstore.level0.dirs.path`,"
            + "`middle`:`/var/folders/zk/l642b_mn4097j57qt1wgk3mm0000gn/T/alluxio-tests/test"
            + "-cluster-f7fa676e-37f8-4ceb-bf7c-c07bb5e3465b/ramdisk`,`right`:`RUNTIME`},"
            + "{`left`:`alluxio.worker.tieredstore.level0.dirs.quota`,`middle`:`${alluxio.worker"
            + ".memory.size}`,`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level0"
            + ".reserved.ratio`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".tieredstore.level0.watermark.high.ratio`,`middle`:`0.95`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.worker.tieredstore.level0.watermark.low.ratio`,`middle`:`0.7`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level1.alias`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level1.dirs.path`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level1.dirs"
            + ".quota`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level1"
            + ".reserved.ratio`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".tieredstore.level1.watermark.high.ratio`,`middle`:`0.95`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.worker.tieredstore.level1.watermark.low.ratio`,`middle`:`0.7`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level2.alias`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level2.dirs.path`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level2.dirs"
            + ".quota`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.level2"
            + ".reserved.ratio`,`middle`:``,`right`:`DEFAULT`},{`left`:`alluxio.worker"
            + ".tieredstore.level2.watermark.high.ratio`,`middle`:`0.95`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.worker.tieredstore.level2.watermark.low.ratio`,`middle`:`0.7`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.levels`,`middle`:`1`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.reserver.enabled`,"
            + "`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore.reserver"
            + ".interval`,`middle`:`1sec`,`right`:`DEFAULT`},{`left`:`alluxio.worker.tieredstore"
            + ".retry`,`middle`:`3`,`right`:`DEFAULT`},{`left`:`alluxio.worker.ufs.block.open"
            + ".timeout`,`middle`:`5min`,`right`:`DEFAULT`},{`left`:`alluxio.worker.ufs.instream"
            + ".cache.enabled`,`middle`:`true`,`right`:`DEFAULT`},{`left`:`alluxio.worker.ufs"
            + ".instream.cache.expiration.time`,`middle`:`5min`,`right`:`DEFAULT`},"
            + "{`left`:`alluxio.worker.ufs.instream.cache.max.size`,`middle`:`5000`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.web.bind.host`,`middle`:`10.238.141"
            + ".254`,`right`:`RUNTIME`},{`left`:`alluxio.worker.web.hostname`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.worker.web.port`,`middle`:`0`,"
            + "`right`:`RUNTIME`},{`left`:`alluxio.zookeeper.address`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.zookeeper.connection.timeout`,`middle`:`15s`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.zookeeper.election.path`,`middle`:`/election`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.zookeeper.enabled`,`middle`:`false`,"
            + "`right`:`DEFAULT`},{`left`:`alluxio.zookeeper.job.election.path`,"
            + "`middle`:`/job_election`,`right`:`DEFAULT`},{`left`:`alluxio.zookeeper.job.leader"
            + ".path`,`middle`:`/job_leader`,`right`:`DEFAULT`},{`left`:`alluxio.zookeeper.leader"
            + ".inquiry.retry`,`middle`:`10`,`right`:`DEFAULT`},{`left`:`alluxio.zookeeper.leader"
            + ".path`,`middle`:`/leader`,`right`:`DEFAULT`},{`left`:`alluxio.zookeeper.session"
            + ".timeout`,`middle`:`60s`,`right`:`DEFAULT`},{`left`:`aws.accessKeyId`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`aws.secretKey`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`fs.cos.access.key`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.cos.app"
            + ".id`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.cos.connection.max`,"
            + "`middle`:`1024`,`right`:`DEFAULT`},{`left`:`fs.cos.connection.timeout`,"
            + "`middle`:`50sec`,`right`:`DEFAULT`},{`left`:`fs.cos.region`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`fs.cos.secret.key`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`fs.cos.socket.timeout`,`middle`:`50sec`,`right`:`DEFAULT`},{`left`:`fs"
            + ".gcs.accessKeyId`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.gcs.secretAccessKey`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`fs.kodo.accesskey`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`fs.kodo.secretkey`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`fs.oss.accessKeyId`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.oss"
            + ".accessKeySecret`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.oss.endpoint`,"
            + "`middle`:``,`right`:`DEFAULT`},{`left`:`fs.swift.apikey`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`fs.swift.auth.method`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`fs.swift.auth.url`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.swift"
            + ".password`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.swift.region`,`middle`:``,"
            + "`right`:`DEFAULT`},{`left`:`fs.swift.simulation`,`middle`:``,`right`:`DEFAULT`},"
            + "{`left`:`fs.swift.tenant`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.swift.use"
            + ".public.url`,`middle`:``,`right`:`DEFAULT`},{`left`:`fs.swift.user`,`middle`:``,"
            + "`right`:`DEFAULT`}],`whitelist`:[`/`]}").replace('`', '"');
    MasterWebUIConfiguration expected =
        new ObjectMapper().readValue(expectedJson, MasterWebUIConfiguration.class);
    String findIgnored = "(test-cluster-).{36}";
    String replaceIgnored = "$1";
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected)
        .replaceAll(findIgnored, replaceIgnored);
    String resultString = new ObjectMapper().writer().writeValueAsString(result)
        .replaceAll(findIgnored, replaceIgnored);
    assertEquals(expectedString, resultString);
  }

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
    assertEquals(expectedString, resultString);
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
    assertEquals(expectedString, resultString);
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
    assertEquals(expectedString, resultString);
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
    String findIgnored = "(test-cluster-).{36}";
    String replaceIgnored = "$1";
    String findPorts = ".port\",\"middle\":\"\\d+\",";
    String replacePorts = ".port\",\"middle\":\"<redacted>\",";
    String expectedString = new ObjectMapper().writer().writeValueAsString(expected)
        .replaceAll(findIgnored, replaceIgnored).replaceAll(findPorts, replacePorts);
    String resultString = new ObjectMapper().writer().writeValueAsString(result)
        .replaceAll(findIgnored, replaceIgnored).replaceAll(findPorts, replacePorts);
    assertEquals(expectedString, resultString);
  }

  // TODO(william) - finish up this test.
  //
  //    private MasterWebUIOverview getWebUIOverviewData() throws Exception {
  //      String result = new TestCase(mHostname, mPort,
  //          getEndpoint(AlluxioMasterRestServiceHandler.WEBUI_OVERVIEW), NO_PARAMS, HttpMethod
  //          .GET,
  //          null).call();
  //      MasterWebUIOverview data = new ObjectMapper().readValue(result, MasterWebUIOverview
  //      .class);
  //      return data;
  //    }
  //
  //  @Test
  //  public void WebUIOverview() throws Exception {
  //    MasterWebUIOverview result = getWebUIOverviewData();
  //    String expectedJson = ("{}").replace('`', '"');
  //    MasterWebUIOverview expected =
  //        new ObjectMapper().readValue(expectedJson, MasterWebUIOverview.class);
  //    String expectedString = new ObjectMapper().writer().writeValueAsString(expected);
  //    String resultString = new ObjectMapper().writer().writeValueAsString(result);
  //    assertEquals(expectedString, resultString);
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
    assertEquals(expectedString, resultString);
  }
}
