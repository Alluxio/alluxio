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

package alluxio.client.cli.fsadmin.command;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.SystemErrRule;
import alluxio.SystemOutRule;
import alluxio.cli.fsadmin.FileSystemAdminShell;
import alluxio.cli.fsadmin.journal.QuorumCommand;
import alluxio.cli.fsadmin.journal.QuorumElectCommand;
import alluxio.cli.fsadmin.journal.QuorumInfoCommand;
import alluxio.cli.fsadmin.journal.QuorumRemoveCommand;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.grpc.JournalDomain;
import alluxio.grpc.QuorumServerInfo;
import alluxio.grpc.QuorumServerState;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Integration tests for the embedded journal.
 */
public final class QuorumCommandIntegrationTest extends BaseIntegrationTest {
  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(
      PropertyKey.USER_METRICS_COLLECTION_ENABLED, "false", ServerConfiguration.global());

  public MultiProcessCluster mCluster;

  public ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
  public ByteArrayOutputStream mErrOutput = new ByteArrayOutputStream();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Rule
  public SystemOutRule mOutRule = new SystemOutRule(mOutput);

  @Rule
  public SystemErrRule mErrRule = new SystemErrRule(mErrOutput);

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
  }

  @Test
  public void quorumInfo() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.QUORUM_SHELL_INFO)
        .setClusterName("QuorumShellInfo").setNumMasters(3).setNumWorkers(0)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        // To make the test run faster.
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms").build();
    mCluster.start();

    String output;

    try (FileSystemAdminShell shell = new FileSystemAdminShell(ServerConfiguration.global())) {
      // Validate quorum state is dumped as expected.
      mOutput.reset();
      shell.run("journal", "quorum", "info", "-domain", "MASTER");
      output = mOutput.toString().trim();
      Assert.assertTrue(output.contains(
          String.format(QuorumInfoCommand.OUTPUT_HEADER_DOMAIN, JournalDomain.MASTER.name())));
      Assert.assertTrue(
          output.contains(String.format(QuorumInfoCommand.OUTPUT_HEADER_QUORUM_SIZE, 3)));
      String journalAddresses =
          ServerConfiguration.get(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES);
      for (String address : journalAddresses.split(",")) {
        String format = String.format(QuorumInfoCommand.OUTPUT_SERVER_INFO,
                QuorumServerState.AVAILABLE.name(), "0", address).trim();
        Assert.assertTrue(output.contains(format));
      }

      // Validate quorum state is updated as expected after a fail-over.
      mCluster.stopMaster(0);
      // Wait for up to 2 election timeouts until quorum notices the change.
      // Use shell "quorum info" for validation.
      CommonUtils.waitFor("Quorum noticing change.", () -> {
        mOutput.reset();
        shell.run("journal", "quorum", "info", "-domain", "MASTER");
        return mOutput.toString().trim().contains(QuorumServerState.UNAVAILABLE.name());
      }, WaitForOptions.defaults().setTimeoutMs(2 * (int) ServerConfiguration.getMs(
          PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT)));
    }
    mCluster.notifySuccess();
  }

  @Test
  public void quorumRemove() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.QUORUM_SHELL_REMOVE)
        .setClusterName("QuorumShellRemove").setNumMasters(5).setNumWorkers(0)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        // To make the test run faster.
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms").build();
    mCluster.start();

    String output;

    try (FileSystemAdminShell shell = new FileSystemAdminShell(ServerConfiguration.global())) {
      AlluxioURI testDir = new AlluxioURI("/testDir");
      mCluster.getFileSystemClient().createDirectory(testDir);
      // Verify cluster is reachable.
      Assert.assertTrue(mCluster.getFileSystemClient().exists(testDir));

      // Validate quorum state is updated as expected after shutting down 2 masters.
      mCluster.stopMaster(0);
      mCluster.stopMaster(1);

      // Verify cluster is reachable.
      Assert.assertTrue(mCluster.getFileSystemClient().exists(testDir));

      // Wait for up to 2 election timeouts until quorum notices changes.
      CommonUtils.waitFor("Quorum noticing change.", () -> {
        try {
          return mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList()
              .stream().filter((info) -> info.getServerState() == QuorumServerState.UNAVAILABLE)
              .collect(Collectors.toList()).size() == 2;
        } catch (Exception e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(6 * (int) ServerConfiguration.getMs(
          PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT)));

      // Remove unavailable masters using shell.
      for (QuorumServerInfo serverInfo : mCluster.getJournalMasterClientForMaster().getQuorumInfo()
          .getServerInfoList()) {
        if (serverInfo.getServerState() == QuorumServerState.UNAVAILABLE) {
          mOutput.reset();

          String serverAddress = String.format("%s:%d", serverInfo.getServerAddress().getHost(),
              serverInfo.getServerAddress().getRpcPort());
          shell.run("journal", "quorum", "remove", "-domain", "MASTER", "-address", serverAddress);

          output = mOutput.toString().trim();
          Assert.assertEquals(String.format(QuorumRemoveCommand.OUTPUT_RESULT, serverAddress,
              JournalDomain.MASTER.name()), lastLine(output));
        }
      }

      // Verify quorum size is resized down to 3 after removing 2 masters.
      Assert.assertEquals(3,
          mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList().size());

      mCluster.stopMaster(2);
      // Verify cluster is reachable.
      Assert.assertTrue(mCluster.getFileSystemClient().exists(testDir));
    }
    mCluster.notifySuccess();
  }

  @Test
  public void elect() throws Exception {
    final int MASTER_INDEX_WAIT_TIME = 5_000;
    int numMasters = 3;
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.QUORUM_SHELL_REMOVE)
            .setClusterName("QuorumShellElect").setNumMasters(numMasters).setNumWorkers(0)
            .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
            .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
            // To make the test run faster.
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
            .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
            .build();
    mCluster.start();

    try (FileSystemAdminShell shell = new FileSystemAdminShell(ServerConfiguration.global())) {
      int newLeaderIdx = (mCluster.getPrimaryMasterIndex(MASTER_INDEX_WAIT_TIME) + 1) % numMasters;
      // `getPrimaryMasterIndex` uses the same `mMasterAddresses` variable as getMasterAddresses
      // we can therefore access to the new leader's address this ways
      MasterNetAddress netAddress = mCluster.getMasterAddresses().get(newLeaderIdx);
      String newLeaderAddr = String.format("%s:%s", netAddress.getHostname(),
              netAddress.getEmbeddedJournalPort());

      mOutput.reset();
      shell.run("journal", "quorum", "elect", "-address" , newLeaderAddr);
      String output = mOutput.toString().trim();
      String expected = String.format(QuorumElectCommand.TRANSFER_SUCCESS, newLeaderAddr);
      Assert.assertEquals(expected, output);
    }
    mCluster.notifySuccess();
  }

  @Test
  public void quorumCommand() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.QUORUM_SHELL)
        .setClusterName("QuorumShell").setNumMasters(3).setNumWorkers(0)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .addProperty(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
        // To make the test run faster.
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
        .addProperty(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms").build();
    mCluster.start();
    try (FileSystemAdminShell shell = new FileSystemAdminShell(ServerConfiguration.global())) {
      String output;

      // Validate quorum sub-commands are validated.
      mOutput.reset();
      shell.run("journal", "quorum", "nonexistentCommand");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumCommand.description(), lastLine(output));

      // Validate option counts are validated for "quorum info"
      mOutput.reset();
      shell.run("journal", "quorum", "info");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumInfoCommand.description(), lastLine(output));

      mOutput.reset();
      shell.run("journal", "quorum", "info", "-op1", "val1", "-op2", "val2");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumInfoCommand.description(), lastLine(output));

      // Validate option counts are validated for "quorum", "remove"
      mOutput.reset();
      shell.run("journal", "quorum", "remove");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumRemoveCommand.description(), lastLine(output));
      mOutput.reset();
      shell.run("journal", "quorum", "remove", "-op1", "val1");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumRemoveCommand.description(), lastLine(output));
      mOutput.reset();
      shell.run("journal", "quorum", "remove", "-op1", "val1", "-op2", "val2", "-op3", "val3");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumRemoveCommand.description(), lastLine(output));

      // Validate option counts are validated for "quorum", "elect"
      mOutput.reset();
      shell.run("journal", "quorum", "elect");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumElectCommand.description(), lastLine(output));
      mOutput.reset();
      shell.run("journal", "quorum", "elect", "-op1", "val1");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumElectCommand.description(), lastLine(output));
      mOutput.reset();
      shell.run("journal", "quorum", "elect", "-op1", "val1", "-op2", "val2", "-op3",
              "val3");
      output = mOutput.toString().trim();
      Assert.assertEquals(QuorumElectCommand.description(), lastLine(output));

      // Validate option validation works for "quorum info".
      mOutput.reset();
      shell.run("journal", "quorum", "info", "-domain", "UNKNOWN");
      output = mOutput.toString().trim();
      Assert.assertEquals(ExceptionMessage.INVALID_OPTION_VALUE.getMessage(
          QuorumInfoCommand.DOMAIN_OPTION_NAME, Arrays.toString(JournalDomain.values())), output);

      // Validate option validation works for "journal quorum remove"
      // Validate -domain is validated.
      mOutput.reset();
      shell.run("journal", "quorum", "remove", "-domain", "UNKNOWN", "-address", "host:0");
      output = mOutput.toString().trim();
      Assert.assertEquals(ExceptionMessage.INVALID_OPTION_VALUE.getMessage(
          QuorumInfoCommand.DOMAIN_OPTION_NAME, Arrays.toString(JournalDomain.values())), output);

      // Validate -address is validated.
      mOutput.reset();
      shell.run("journal", "quorum", "remove", "-domain", "JOB_MASTER", "-address",
          "hostname:invalid_port");
      output = mOutput.toString().trim();
      Assert.assertEquals(ExceptionMessage.INVALID_ADDRESS_VALUE.getMessage(), output);

      // Validate -address is validated for transferLeader.
      mOutput.reset();
      shell.run("journal", "quorum", "elect", "-address", "hostname:invalid_port");
      output = mOutput.toString().trim();
      Assert.assertEquals(ExceptionMessage.INVALID_ADDRESS_VALUE.getMessage(), output);
    }
    mCluster.notifySuccess();
  }

  private String lastLine(String output) {
    String[] lines = output.split("\n");
    if (lines.length > 0) {
      return lines[lines.length - 1];
    }
    return "";
  }
}

