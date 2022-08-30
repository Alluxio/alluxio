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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.NodeState;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalLogWriter;
import alluxio.master.journal.ufs.UfsJournalSingleMasterPrimarySelector;
import alluxio.master.journal.ufs.UfsJournalSystem;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for {@link AlluxioMasterProcess}.
 */
@RunWith(Parameterized.class)
public final class AlluxioMasterProcessTest {
  @Rule
  public PortReservationRule mRpcPortRule = new PortReservationRule();
  @Rule
  public PortReservationRule mWebPortRule = new PortReservationRule();
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {new ImmutableMap.Builder<Object, Boolean>()
            .put(PropertyKey.STANDBY_MASTER_WEB_ENABLED, true)
            .put(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED, true)
            .build()},
        {new ImmutableMap.Builder<Object, Boolean>()
            .put(PropertyKey.STANDBY_MASTER_WEB_ENABLED, false)
            .put(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED, false)
            .build()},
        {new ImmutableMap.Builder<Object, Boolean>()
            .put(PropertyKey.STANDBY_MASTER_WEB_ENABLED, true)
            .put(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED, false)
            .build()},
        {new ImmutableMap.Builder<Object, Boolean>()
            .put(PropertyKey.STANDBY_MASTER_WEB_ENABLED, false)
            .put(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED, true)
            .build()},
    });
  }

  public ImmutableMap<PropertyKey, Object> mConfigMap;

  public AlluxioMasterProcessTest(ImmutableMap<PropertyKey, Object> propMap) {
    mConfigMap = propMap;
  }

  @Before
  public void before() throws Exception {
    Configuration.reloadProperties();
    Configuration.set(PropertyKey.MASTER_RPC_PORT, mRpcPortRule.getPort());
    Configuration.set(PropertyKey.MASTER_WEB_PORT, mWebPortRule.getPort());
    Configuration.set(PropertyKey.MASTER_METASTORE_DIR, mFolder.newFolder("metastore"));
    Configuration.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false);
    Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, mFolder.newFolder("journal"));
    for (Map.Entry<PropertyKey, Object> entry : mConfigMap.entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }
  }

  @Test
  public void startStopPrimary() throws Exception {
    AlluxioMasterProcess master = new AlluxioMasterProcess(new NoopJournalSystem(),
        new UfsJournalSingleMasterPrimarySelector());
    Thread t = new Thread(() -> {
      try {
        master.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    startStopTest(master);
  }

  @Test
  public void startStopStandby() throws Exception {
    AlluxioMasterProcess master = new AlluxioMasterProcess(
        new NoopJournalSystem(), new AlwaysStandbyPrimarySelector());
    Thread t = new Thread(() -> {
      try {
        master.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    startStopTest(master, false,
        Configuration.getBoolean(PropertyKey.STANDBY_MASTER_WEB_ENABLED),
        Configuration.getBoolean(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED));
  }

  @Test
  public void startMastersThrowsUnavailableException() throws InterruptedException, IOException {
    ControllablePrimarySelector primarySelector = new ControllablePrimarySelector();
    primarySelector.setState(NodeState.PRIMARY);
    Configuration.set(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION, true);
    AlluxioMasterProcess master = new AlluxioMasterProcess(
        new NoopJournalSystem(), primarySelector);
    AlluxioMasterProcess spy = Mockito.spy(master);
    Mockito.doAnswer(invocation -> { throw new UnavailableException("unavailable"); })
        .when(spy).startMasters(true);

    AtomicBoolean success = new AtomicBoolean(true);
    Thread t = new Thread(() -> {
      try {
        spy.start();
      } catch (UnavailableException ue) {
        success.set(false);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    final int WAIT_TIME_TO_THROW_EXC = 500; // in ms
    t.join(WAIT_TIME_TO_THROW_EXC);
    t.interrupt();
    Assert.assertTrue(success.get());
  }

  @Test
  public void failToGainPrimacyWhenJournalCorrupted() throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Configuration.set(PropertyKey.MASTER_JOURNAL_BACKUP_WHEN_CORRUPTED, false);
    URI journalLocation = JournalUtils.getJournalLocation();
    JournalSystem journalSystem = new JournalSystem.Builder()
        .setLocation(journalLocation).build(CommonUtils.ProcessType.MASTER);
    AlluxioMasterProcess masterProcess = new AlluxioMasterProcess(journalSystem,
        new UfsJournalSingleMasterPrimarySelector());
    corruptJournalAndStartMasterProcess(masterProcess, journalLocation);
  }

  @Test
  public void failToGainPrimacyWhenJournalCorruptedHA() throws Exception {
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Configuration.set(PropertyKey.MASTER_JOURNAL_BACKUP_WHEN_CORRUPTED, false);
    URI journalLocation = JournalUtils.getJournalLocation();
    JournalSystem journalSystem = new JournalSystem.Builder()
        .setLocation(journalLocation).build(CommonUtils.ProcessType.MASTER);
    ControllablePrimarySelector primarySelector = new ControllablePrimarySelector();
    AlluxioMasterProcess masterProcess =
        new AlluxioMasterProcess(journalSystem, primarySelector);
    primarySelector.setState(NodeState.PRIMARY);
    corruptJournalAndStartMasterProcess(masterProcess, journalLocation);
  }

  private void corruptJournalAndStartMasterProcess(AlluxioMasterProcess masterProcess,
      URI journalLocation) throws Exception {
    assertTrue(masterProcess.mJournalSystem instanceof UfsJournalSystem);
    masterProcess.mJournalSystem.format();
    // corrupt the journal
    UfsJournal fsMaster =
        new UfsJournal(URIUtils.appendPathOrDie(journalLocation, "FileSystemMaster"),
            new NoopMaster(), 0, Collections::emptySet);
    fsMaster.start();
    fsMaster.gainPrimacy();
    long nextSN = 0;
    try (UfsJournalLogWriter writer = new UfsJournalLogWriter(fsMaster, nextSN)) {
      Journal.JournalEntry entry = Journal.JournalEntry.newBuilder()
          .setSequenceNumber(nextSN)
          .setDeleteFile(File.DeleteFileEntry.newBuilder()
              .setId(4563728) // random non-zero ID number (zero would delete the root)
              .setPath("/nonexistant")
              .build())
          .build();
      writer.write(entry);
      writer.flush();
    }
    // comes from mJournalSystem#gainPrimacy
    RuntimeException exception = assertThrows(RuntimeException.class, masterProcess::start);
    assertTrue(exception.getMessage().contains(NoSuchElementException.class.getName()));
    // if AlluxioMasterProcess#start throws an exception, then #stop will get called
    masterProcess.stop();
    assertTrue(masterProcess.isStopped());
  }

  @Test
  @Ignore
  public void stopAfterStandbyTransition() throws Exception {
    ControllablePrimarySelector primarySelector = new ControllablePrimarySelector();
    primarySelector.setState(NodeState.PRIMARY);
    Configuration.set(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION, true);
    AlluxioMasterProcess master = new AlluxioMasterProcess(
        new NoopJournalSystem(), primarySelector);
    Thread t = new Thread(() -> {
      try {
        master.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    waitForSocketServing(ServiceType.MASTER_RPC);
    waitForSocketServing(ServiceType.MASTER_WEB);
    int rpcPort = master.getRpcAddress().getPort();
    int webPort = master.getWebAddress().getPort();
    assertTrue(isBound(rpcPort));
    assertTrue(isBound(webPort));
    primarySelector.setState(NodeState.STANDBY);
    t.join(10000);
    CommonUtils.waitFor("Master to be stopped", () -> !master.isRunning(),
        WaitForOptions.defaults().setTimeoutMs(3 * Constants.MINUTE_MS));
    CommonUtils.waitFor("Master to be stopped", () -> !isBound(rpcPort),
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
    CommonUtils.waitFor("Master to be stopped", () -> !isBound(webPort),
        WaitForOptions.defaults().setTimeoutMs(Constants.MINUTE_MS));
  }

  /**
   * This test ensures that given a root UFS which is <i>not</i> local, that we can still provide a
   * local path to an Alluxio backup and start the master.
   *
   * This test is ignored because it requires adding a new UFS dependency in the test scope, but
   * adding the alluxio-underfs-web dependency causes the module to hang in the  builds even after
   * the test completes. The hang seems to be due to a bug in maven 3.5.4.
   */
  @Test
  @Ignore
  public void restoreFromBackupLocal() throws Exception {
    URL backupResource = this.getClass().getResource("/alluxio-local-backup.gz");
    Preconditions.checkNotNull(backupResource);
    String backupPath = backupResource.toURI().toString();
    String journalPath = PathUtils.concatPath(mFolder.getRoot(), "journal");
    FileUtils.createDir(journalPath);
    String ufsPath = PathUtils.concatPath(mFolder.getRoot(), "ufs");
    FileUtils.createDir(ufsPath);
    ufsPath = "http://other_ufs/";
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "550");
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1100");
    Configuration.set(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP, backupPath);
    Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, journalPath);
    Configuration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, ufsPath);
    AlluxioMasterProcess master = new AlluxioMasterProcess(
        new RaftJournalSystem(JournalUtils.getJournalLocation(), ServiceType.MASTER_RAFT),
        new UfsJournalSingleMasterPrimarySelector());
    Thread t = new Thread(() -> {
      try {
        master.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    startStopTest(master);
  }

  private void startStopTest(AlluxioMasterProcess master) throws Exception {
    startStopTest(master, true, true, true);
  }

  private void startStopTest(AlluxioMasterProcess master, boolean expectGrpcServiceStarted,
      boolean expectWebServiceStarted, boolean expectMetricsSinkStarted) throws Exception {
    final int TIMEOUT_MS = 5_000;
    // rpc and web ports will be bound either by the serving server, or by the rejecting server
    waitForSocketServing(ServiceType.MASTER_RPC);
    waitForSocketServing(ServiceType.MASTER_WEB);
    assertTrue(isBound(master.getRpcAddress().getPort()));
    assertTrue(isBound(master.getWebAddress().getPort()));
    if (expectGrpcServiceStarted) {
      assertTrue(master.waitForGrpcServerReady(TIMEOUT_MS));
    }
    if (expectWebServiceStarted) {
      assertTrue(master.waitForWebServerReady(TIMEOUT_MS));
    }
    if (expectMetricsSinkStarted) {
      assertTrue(master.waitForMetricSinkServing(TIMEOUT_MS));
    }
    master.stop();
    assertFalse(isBound(master.getRpcAddress().getPort()));
    assertFalse(isBound(master.getWebAddress().getPort()));
  }

  private void waitForSocketServing(ServiceType service)
      throws TimeoutException, InterruptedException {
    InetSocketAddress addr =
        NetworkAddressUtils.getBindAddress(service, Configuration.global());
    CommonUtils.waitFor(service + " to be serving", () -> {
      try {
        Socket s = new Socket(addr.getAddress(), addr.getPort());
        s.close();
        return true;
      } catch (ConnectException e) {
        return false;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(5 * Constants.MINUTE_MS));
  }

  private boolean isBound(int port) {
    try {
      ServerSocket socket = new ServerSocket(port);
      socket.setReuseAddress(true);
      socket.close();
      return false;
    } catch (BindException e) {
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
