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

package alluxio.client.cli;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.SystemOutRule;
import alluxio.client.file.FileSystem;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.MasterClientContext;
import alluxio.master.SingleMasterInquireClient;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftJournalUtils;
import alluxio.master.journal.tool.JournalTool;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;

import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageImpl;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link JournalTool}.
 */
public class JournalToolTest extends BaseIntegrationTest {
  private static final int CHECKPOINT_SIZE = 100;

  private final ByteArrayOutputStream mOutput = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mSystemOutRule = new SystemOutRule(mOutput);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setIncludeSecondary(true)
          .setProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
          .setProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES,
              Integer.toString(CHECKPOINT_SIZE))
          .setProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "100")
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "MUST_CACHE").build();

  private File mDumpDir;
  private FileSystem mFs;

  @Before
  public void before() throws IOException {
    mDumpDir = AlluxioTestDirectory.createTemporaryDirectory("journal_dump");
    mFs = mLocalAlluxioClusterResource.get().getClient();
  }

  @Test
  public void dumpSimpleUfsJournal() throws Throwable {
    // Create a test directory to trigger journaling.
    mFs.createDirectory(new AlluxioURI("/test"));
    // Run journal tool.
    JournalTool.main(new String[] {"-outputDir", mDumpDir.getAbsolutePath()});
    // Verify that a non-zero dump file exists.
    assertThat(mOutput.toString(), containsString(mDumpDir.getAbsolutePath()));
    assertNonemptyFileExists(PathUtils.concatPath(mDumpDir, "edits.txt"));
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.MASTER_JOURNAL_TYPE, "EMBEDDED"})
  public void dumpSimpleEmbeddedJournal() throws Throwable {
    // Create a test directory to trigger journaling.
    mFs.createDirectory(new AlluxioURI("/test"));
    mLocalAlluxioClusterResource.get().stopMasters();
    // Run journal tool.
    String masterJournalPath =
        mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getJournalFolder();
    JournalTool.main(
        new String[] {"-inputDir", masterJournalPath, "-outputDir", mDumpDir.getAbsolutePath()});
    // Verify that a non-zero dump file exists.
    assertThat(mOutput.toString(), containsString(mDumpDir.getAbsolutePath()));
    assertNonemptyFileExists(PathUtils.concatPath(mDumpDir, "edits.txt"));
  }

  @Test
  public void dumpHeapCheckpointFromUfsJournal() throws Throwable {
    for (String name : Arrays.asList("/pin", "/max_replication", "/async_persist", "/ttl")) {
      mFs.createFile(new AlluxioURI(name)).close();
    }
    mFs.setAttribute(new AlluxioURI("/pin"),
        SetAttributePOptions.newBuilder().setPinned(true).build());
    mFs.setAttribute(new AlluxioURI("/max_replication"),
        SetAttributePOptions.newBuilder().setReplicationMax(5).build());
    mFs.persist(new AlluxioURI("/async_persist"));
    mFs.setAttribute(new AlluxioURI("/ttl"),
        SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(100000).build())
            .build());
    checkpointUfsJournal();
    JournalTool.main(new String[] {"-outputDir", mDumpDir.getAbsolutePath()});
    String checkpointDir = findCheckpointDir();

    assertNonemptyFileExists(PathUtils.concatPath(mDumpDir, "edits.txt"));
    assertNonemptyFileExists(PathUtils.concatPath(checkpointDir, "INODE_DIRECTORY_ID_GENERATOR"));
    for (String subPath : Arrays.asList("INODE_COUNTER", "PINNED_INODE_FILE_IDS",
        "REPLICATION_LIMITED_FILE_IDS", "TO_BE_PERSISTED_FILE_IDS")) {
      assertNonemptyFileExists(PathUtils.concatPath(checkpointDir, "INODE_TREE", subPath));
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.MASTER_JOURNAL_TYPE,
      "EMBEDDED", PropertyKey.Name.MASTER_METASTORE, "HEAP"})
  public void dumpHeapCheckpointFromEmbeddedJournal() throws Throwable {
    for (String name : Arrays.asList("/pin", "/max_replication", "/async_persist", "/ttl")) {
      mFs.createFile(new AlluxioURI(name)).close();
    }
    mFs.setAttribute(new AlluxioURI("/pin"),
        SetAttributePOptions.newBuilder().setPinned(true).build());
    mFs.setAttribute(new AlluxioURI("/max_replication"),
        SetAttributePOptions.newBuilder().setReplicationMax(5).build());
    mFs.persist(new AlluxioURI("/async_persist"));
    mFs.setAttribute(new AlluxioURI("/ttl"),
        SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(100000).build())
            .build());
    checkpointEmbeddedJournal();

    mLocalAlluxioClusterResource.get().stopMasters();
    // Read current snapshot index.
    final long snapshotIdxTarget = CHECKPOINT_SIZE * 2 + 50;
    long currentSnapshotIdx = getCurrentRatisSnapshotIndex(
        mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getJournalFolder());
    // Verify it's beyond target.
    assertTrue("snapshot should be beyond target", currentSnapshotIdx >= snapshotIdxTarget);
    // Make sure to point the tool to leader's journal folder.
    String leaderJournalDir =
        mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getJournalFolder();
    // Run journal tool.
    JournalTool.main(
        new String[] {"-inputDir", leaderJournalDir, "-outputDir", mDumpDir.getAbsolutePath()});
    // Find the main checkpoint dir.
    String checkpointDir = findCheckpointDir();
    // Embedded journal checkpoints are grouped by masters.
    String fsMasterCheckpointsDir = PathUtils.concatPath(checkpointDir, "FILE_SYSTEM_MASTER");

    assertNonemptyFileExists(
        PathUtils.concatPath(fsMasterCheckpointsDir, "INODE_DIRECTORY_ID_GENERATOR"));
    for (String subPath : Arrays.asList("HEAP_INODE_STORE", "INODE_COUNTER",
        "PINNED_INODE_FILE_IDS", "REPLICATION_LIMITED_FILE_IDS", "TO_BE_PERSISTED_FILE_IDS")) {
      assertNonemptyFileExists(PathUtils.concatPath(fsMasterCheckpointsDir, "INODE_TREE", subPath));
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.MASTER_METASTORE, "ROCKS"})
  public void dumpRocksCheckpointFromUfsJournal() throws Throwable {
    checkpointUfsJournal();
    JournalTool.main(new String[] {"-outputDir", mDumpDir.getAbsolutePath()});
    String checkpointDir = findCheckpointDir();
    assertNonemptyFileExists(
        PathUtils.concatPath(checkpointDir, "INODE_TREE", "CACHING_INODE_STORE"));
  }

  private void checkpointUfsJournal() throws Exception {
    // Perform operations to generate a checkpoint.
    for (int i = 0; i < CHECKPOINT_SIZE * 2; i++) {
      mFs.createFile(new AlluxioURI("/" + i)).close();
    }
    IntegrationTestUtils.waitForUfsJournalCheckpoint(Constants.FILE_SYSTEM_MASTER_NAME);
  }

  private void checkpointEmbeddedJournal() throws Throwable {
    // Perform operations before generating a checkpoint.
    for (int i = 0; i < CHECKPOINT_SIZE * 2; i++) {
      mFs.createFile(new AlluxioURI("/" + i)).close();
    }

    // Take snapshot on master.
    new RetryHandlingMetaMasterClient(MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global()))
        .setMasterInquireClient(new SingleMasterInquireClient(
            mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getAddress()))
        .build()).checkpoint();
  }

  private long getCurrentRatisSnapshotIndex(String journalFolder) throws Throwable {
    try (RaftStorage storage = new RaftStorageImpl(
        new File(RaftJournalUtils.getRaftJournalDir(new File(journalFolder)),
            RaftJournalSystem.RAFT_GROUP_UUID.toString()),
            RaftServerConfigKeys.Log.CorruptionPolicy.getDefault())) {
      SimpleStateMachineStorage stateMachineStorage = new SimpleStateMachineStorage();
      stateMachineStorage.init(storage);
      SingleFileSnapshotInfo snapshot = stateMachineStorage.getLatestSnapshot();
      if (snapshot == null) {
        throw new IOException("Failed to find a valid snapshot");
      }
      return snapshot.getIndex();
    }
  }

  private String findCheckpointDir() throws IOException {
    List<Path> checkpoint = Files.list(mDumpDir.toPath())
        .filter(p -> p.toString().contains("checkpoints-")).collect(toList());
    assertEquals("Unexpected checkpoint list: " + checkpoint, 1, checkpoint.size());
    return checkpoint.get(0).toString();
  }

  private void assertNonemptyFileExists(String s) {
    File f = new File(s);
    assertTrue(f.exists());
    assertTrue(f.isFile());
    assertThat(f.length(), Matchers.greaterThan(0L));
  }

  private void assertNonemptyDirExists(String s) {
    File f = new File(s);
    assertTrue(f.exists());
    assertTrue(f.isDirectory());
    assertThat(f.list().length, Matchers.greaterThan(0));
  }
}
