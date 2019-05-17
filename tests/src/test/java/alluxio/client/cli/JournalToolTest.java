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
import alluxio.Constants;
import alluxio.SystemOutRule;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.journal.JournalTool;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.util.io.PathUtils;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link JournalTool}.
 */
public class JournalToolTest extends BaseIntegrationTest {
  private static final int CHECKPOINT_SIZE = 100;
  private static final int LOG_SIZE_BYTES_MAX = 100;
  private static final int MASTER_COUNT = 2;
  private static final int WORKER_COUNT = 1;
  private static final int GET_MASTER_INDEX_TIMEOUT_MS = 10 * 1000;
  private static final String PERSISTENCE_INITIAL_INTERVAL_TIME = "1min";

  private final ByteArrayOutputStream mOutput = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mSystemOutRule = new SystemOutRule(mOutput);

  private MultiProcessCluster mMultiProcessCluster;
  private File mDumpDir;
  private FileSystem mFs;

  private void initializeCluster(Map<PropertyKey, String> props) throws Throwable {
    // Initialize default properties.
    Map<PropertyKey, String> defaultProps = new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString());
        put(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES,
            Integer.toString(CHECKPOINT_SIZE));
        put(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, Integer.toString(LOG_SIZE_BYTES_MAX));
        put(PropertyKey.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS, PERSISTENCE_INITIAL_INTERVAL_TIME);
      }
    };
    // Override/merge with given props.
    if (props != null) {
      defaultProps.putAll(props);
    }
    // Build and start a multi-process cluster.
    mMultiProcessCluster = MultiProcessCluster
        .newBuilder(PortCoordination.JOURNAL_TOOL)
        .setNumMasters(MASTER_COUNT)
        .setNumWorkers(WORKER_COUNT)
        .addProperties(defaultProps)
        .build();
    mMultiProcessCluster.start();
    // Acquire FS client.
    mFs = mMultiProcessCluster.getFileSystemClient();
    // Ensure directory for dumping journal.
    mDumpDir = AlluxioTestDirectory.createTemporaryDirectory("journal_dump");
  }

  @After
  public void after() throws Throwable {
    mMultiProcessCluster.destroy();
  }

  @Test
  public void dumpSimpleUfsJournal() throws Throwable {
    initializeCluster(null);
    // Create a test directory to trigger journaling.
    mFs.createDirectory(new AlluxioURI("/test"));
    // Run journal tool.
    JournalTool.main(new String[]{"-outputDir", mDumpDir.getAbsolutePath()});
    // Verify that a non-zero dump file exists.
    assertThat(mOutput.toString(), containsString(mDumpDir.getAbsolutePath()));
    assertNonemptyFileExists(PathUtils.concatPath(mDumpDir, "edits.txt"));
  }

  @Test
  public void dumpSimpleEmbeddedJournal() throws Throwable {
    initializeCluster(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString());
      }
    });
    // Create a test directory to trigger journaling.
    mFs.createDirectory(new AlluxioURI("/test"));
    // Run journal tool.
    String masterJournalPath = mMultiProcessCluster.getJournalDir()
        + Integer.toString(mMultiProcessCluster.getPrimaryMasterIndex(GET_MASTER_INDEX_TIMEOUT_MS));
    JournalTool.main(new String[] {
        "-inputDir", masterJournalPath,
        "-outputDir", mDumpDir.getAbsolutePath()});
    // Verify that a non-zero dump file exists.
    assertThat(mOutput.toString(), containsString(mDumpDir.getAbsolutePath()));
    assertNonemptyFileExists(PathUtils.concatPath(mDumpDir, "edits.txt"));
  }

  @Test
  public void dumpHeapCheckpointFromUfsJournal() throws Throwable {
    initializeCluster(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.MASTER_METASTORE, "HEAP");
      }
    });

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
    JournalTool.main(new String[]{"-outputDir", mDumpDir.getAbsolutePath()});
    String checkpointDir = findCheckpointDir();

    assertNonemptyFileExists(PathUtils.concatPath(mDumpDir, "edits.txt"));
    assertNonemptyFileExists(PathUtils.concatPath(checkpointDir, "INODE_DIRECTORY_ID_GENERATOR"));
    for (String subPath : Arrays.asList("HEAP_INODE_STORE", "INODE_COUNTER",
        "PINNED_INODE_FILE_IDS", "REPLICATION_LIMITED_FILE_IDS", "TO_BE_PERSISTED_FILE_IDS")) {
      assertNonemptyFileExists(PathUtils.concatPath(checkpointDir, "INODE_TREE", subPath));
    }
  }

  @Test
  public void dumpRocksCheckpointFromUfsJournal() throws Throwable {
    initializeCluster(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.MASTER_METASTORE, "ROCKS");
      }
    });

    checkpointUfsJournal();
    JournalTool.main(new String[] {"-outputDir", mDumpDir.getAbsolutePath()});
    String checkpointDir = findCheckpointDir();
    assertNonemptyDirExists(
        PathUtils.concatPath(checkpointDir, "INODE_TREE", "CACHING_INODE_STORE"));
  }

  private void checkpointUfsJournal() throws Exception {
    // Perform operations to generate a checkpoint.
    for (int i = 0; i < CHECKPOINT_SIZE * 2; i++) {
      mFs.createFile(new AlluxioURI("/" + i)).close();
    }
    IntegrationTestUtils.waitForUfsJournalCheckpoint(Constants.FILE_SYSTEM_MASTER_NAME);
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
