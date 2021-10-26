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
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.master.journal.raft.RaftJournalConfiguration;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.TimeoutException;

/**
 * Tests for {@link AlluxioMasterProcess}.
 */
public final class AlluxioMasterProcessTest {

  @Rule
  public PortReservationRule mRpcPortRule = new PortReservationRule();
  @Rule
  public PortReservationRule mWebPortRule = new PortReservationRule();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  private int mRpcPort;
  private int mWebPort;

  @Before
  public void before() throws Exception {
    ServerConfiguration.reset();
    mRpcPort = mRpcPortRule.getPort();
    mWebPort = mWebPortRule.getPort();
    ServerConfiguration.set(PropertyKey.MASTER_RPC_PORT, mRpcPort);
    ServerConfiguration.set(PropertyKey.MASTER_WEB_PORT, mWebPort);
    ServerConfiguration.set(PropertyKey.MASTER_METASTORE_DIR, mFolder.getRoot().getAbsolutePath());
    String journalPath = PathUtils.concatPath(mFolder.getRoot(), "journal");
    FileUtils.createDir(journalPath);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_FOLDER, journalPath);
  }

  @Test
  public void startStopPrimary() throws Exception {
    AlluxioMasterProcess master = new AlluxioMasterProcess(new NoopJournalSystem());
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
  public void startStopSecondary() throws Exception {
    FaultTolerantAlluxioMasterProcess master = new FaultTolerantAlluxioMasterProcess(
        new NoopJournalSystem(), new AlwaysSecondaryPrimarySelector());
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
  public void stopAfterSecondaryTransition() throws Exception {
    ControllablePrimarySelector primarySelector = new ControllablePrimarySelector();
    primarySelector.setState(PrimarySelector.State.PRIMARY);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION, "true");
    FaultTolerantAlluxioMasterProcess master = new FaultTolerantAlluxioMasterProcess(
        new NoopJournalSystem(), primarySelector);
    Thread t = new Thread(() -> {
      try {
        master.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    waitForServing(ServiceType.MASTER_RPC);
    waitForServing(ServiceType.MASTER_WEB);
    assertTrue(isBound(mRpcPort));
    assertTrue(isBound(mWebPort));
    primarySelector.setState(PrimarySelector.State.SECONDARY);
    t.join(10000);
    // make these two lines flake less
    //assertFalse(isBound(mRpcPort));
    //assertFalse(isBound(mWebPort));
    assertFalse(master.isRunning());
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
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "550");
    ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1100");
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP, backupPath);
    ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_FOLDER, journalPath);
    ServerConfiguration.set(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, ufsPath);
    AlluxioMasterProcess master = new AlluxioMasterProcess(
        RaftJournalSystem.create(RaftJournalConfiguration.defaults(ServiceType.MASTER_RAFT)));
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
  public void startZeroParallelism() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM, "0");
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(String.format("Cannot start Alluxio master gRPC thread pool with "
                    + "%s=%s! The parallelism must be greater than 0!",
            PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM.toString(), 0));
    AlluxioMasterProcess.Factory.create();
  }

  @Test
  public void startNegativeParallelism() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM, "-1");
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(String.format("Cannot start Alluxio master gRPC thread pool with"
                    + " %s=%s! The parallelism must be greater than 0!",
            PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM.toString(), -1));
    AlluxioMasterProcess.Factory.create();
  }

  @Test
  public void startInvalidMaxPoolSize() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM, "4");
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE, "3");
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(String.format("Cannot start Alluxio master gRPC thread pool with "
                    + "%s=%s greater than %s=%s!",
            PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM.toString(), 4,
            PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE.toString(), 3));
    AlluxioMasterProcess.Factory.create();
  }

  @Test
  public void startZeroKeepAliveTime() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE, "0");
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(
            String.format("Cannot start Alluxio master gRPC thread pool with %s=%s. "
                    + "The keepalive time must be greater than 0!",
            PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE.toString(),
            0));
    AlluxioMasterProcess.Factory.create();
  }

  @Test
  public void startNegativeKeepAliveTime() {
    ServerConfiguration.set(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE, "-1");
    mException.expect(IllegalArgumentException.class);
    mException.expectMessage(
            String.format("Cannot start Alluxio master gRPC thread pool with %s=%s. "
                            + "The keepalive time must be greater than 0!",
                    PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE.toString(),
                    -1));
    AlluxioMasterProcess.Factory.create();
  }

  private void startStopTest(AlluxioMasterProcess master) throws Exception {
    waitForServing(ServiceType.MASTER_RPC);
    waitForServing(ServiceType.MASTER_WEB);
    assertTrue(isBound(mRpcPort));
    assertTrue(isBound(mWebPort));
    boolean testMode = ServerConfiguration.getBoolean(PropertyKey.TEST_MODE);
    ServerConfiguration.set(PropertyKey.TEST_MODE, false);
    master.waitForReady(5000);
    ServerConfiguration.set(PropertyKey.TEST_MODE, testMode);
    master.stop();
    assertFalse(isBound(mRpcPort));
    assertFalse(isBound(mWebPort));
  }

  private void waitForServing(ServiceType service) throws TimeoutException, InterruptedException {
    InetSocketAddress addr =
        NetworkAddressUtils.getBindAddress(service, ServerConfiguration.global());
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
