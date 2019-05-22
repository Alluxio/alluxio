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
import alluxio.conf.PropertyKey.Name;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.journal.JournalTool;
import alluxio.master.journal.JournalType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.LocalAlluxioClusterResource.Config;
import alluxio.util.io.PathUtils;

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
          .setProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
          .setProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES,
              Integer.toString(CHECKPOINT_SIZE))
          .setProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "100")
          .build();

  private File mDumpDir;
  private FileSystem mFs;

  @Before
  public void before() throws IOException {
    mDumpDir = AlluxioTestDirectory.createTemporaryDirectory("journal_dump");
    mFs = mLocalAlluxioClusterResource.get().getClient();
  }

  @Test
  public void dumpSimpleJournal() throws IOException {
    JournalTool.main(new String[]{"-outputDir", mDumpDir.getAbsolutePath()});
    assertThat(mOutput.toString(), containsString(mDumpDir.getAbsolutePath()));
    assertNonemptyFileExists(PathUtils.concatPath(mDumpDir, "edits.txt"));
  }

  @Test
  @Config(confParams = {Name.MASTER_METASTORE, "HEAP"})
  public void dumpHeapCheckpoint() throws Exception {
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
    checkpoint();
    JournalTool.main(new String[]{"-outputDir", mDumpDir.getAbsolutePath()});
    String checkpointDir = findCheckpointDir();

    assertNonemptyFileExists(PathUtils.concatPath(mDumpDir, "edits.txt"));
    assertNonemptyFileExists(PathUtils.concatPath(checkpointDir, "INODE_DIRECTORY_ID_GENERATOR"));
    assertNonemptyFileExists(PathUtils.concatPath(checkpointDir, "INODE_TREE", "INODE_COUNTER"));
    for (String subPath : Arrays.asList("HEAP_INODE_STORE_INODES",
        "HEAP_INODE_STORE_INDICES_PINNED", "HEAP_INODE_STORE_INDICES_REPLICATION_LIMITED",
        "HEAP_INODE_STORE_INDICES_TO_BE_PERSISTED")) {
      assertNonemptyFileExists(
          PathUtils.concatPath(checkpointDir, "INODE_TREE", "HEAP_INODE_STORE", subPath));
    }
  }

  @Test
  @Config(confParams = {Name.MASTER_METASTORE, "ROCKS"})
  public void dumpRocksCheckpoint() throws Exception {
    checkpoint();
    JournalTool.main(new String[] {"-outputDir", mDumpDir.getAbsolutePath()});
    String checkpointDir = findCheckpointDir();
    assertNonemptyDirExists(
        PathUtils.concatPath(checkpointDir, "INODE_TREE", "CACHING_INODE_STORE"));
  }

  private void checkpoint() throws Exception {
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
