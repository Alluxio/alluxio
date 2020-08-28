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

package alluxio.master.journal.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.FileSystemMasterFactory;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for {@link JournalSinkTest}.
 */
public final class JournalSinkTest {
  private static final Logger LOG = LoggerFactory.getLogger(JournalSinkTest.class);
  private static final long INVALID_ID = -1;

  private MasterRegistry mRegistry;
  private JournalSystem mJournalSystem;
  private FileSystemMaster mFileSystemMaster;
  private String mJournalFolder;

  private Queue<JournalEntry> mEntries;
  private TestJournalSink mJournalSink;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule = new ConfigurationRule(new HashMap() {
    {
      put(PropertyKey.MASTER_JOURNAL_TYPE, "UFS");
      put(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, "20");
      put(PropertyKey.SECURITY_AUTHENTICATION_TYPE, "NOSASL");
      put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false");
      put(PropertyKey.WORK_DIR,
          AlluxioTestDirectory.createTemporaryDirectory("workdir").getAbsolutePath());
      put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, AlluxioTestDirectory
          .createTemporaryDirectory("FileSystemMasterTest").getAbsolutePath());
    }
  }, ServerConfiguration.global());

  @Before
  public void before() throws Exception {
    mJournalFolder = mTestFolder.newFolder().getAbsolutePath();
    mJournalSystem = JournalTestUtils.createJournalSystem(mJournalFolder);
    mRegistry = new MasterRegistry();

    mEntries = new LinkedBlockingQueue<>();
    mJournalSink = new TestJournalSink(mEntries);

    startMasters(mRegistry, mJournalSystem, mJournalSink, true);
    mFileSystemMaster = mRegistry.get(FileSystemMaster.class);
  }

  @After
  public void after() throws Exception {
    stopMasters(mRegistry, mJournalSystem);
  }

  @Test
  public void writeEvents() throws Exception {
    createFile("/file");
    assertNotEquals(INVALID_ID, findFile("file"));

    createFile("/for_nested_file/nested_file");
    assertNotEquals(INVALID_ID, findDir("for_nested_file"));
    assertNotEquals(INVALID_ID, findFile("nested_file"));

    createDir("/dir");
    assertNotEquals(INVALID_ID, findDir("dir"));

    createDir("/for_nested_dir/nested_dir");
    assertNotEquals(INVALID_ID, findDir("for_nested_dir"));
    assertNotEquals(INVALID_ID, findDir("nested_dir"));

    createFile("/rename_src");
    mFileSystemMaster.rename(new AlluxioURI("/rename_src"), new AlluxioURI("/rename_dst"),
        RenameContext.defaults());
    assertNotEquals(INVALID_ID, findFile("rename_src"));
    assertNotEquals(INVALID_ID, findRename("rename_dst"));

    createFile("/deleted_file");
    mFileSystemMaster.delete(new AlluxioURI("/deleted_file"), DeleteContext.defaults());
    final long deleteId = findFile("deleted_file");
    assertNotEquals(INVALID_ID, deleteId);
    assertNotEquals(INVALID_ID, findDelete(deleteId));

    createFile("/deleted_dir/file1");
    mFileSystemMaster.delete(new AlluxioURI("/deleted_dir"),
        DeleteContext.create(DeletePOptions.newBuilder().setRecursive(true)));
    final long deleteId1 = findFile("file1");
    assertNotEquals(INVALID_ID, deleteId1);
    assertNotEquals(INVALID_ID, findDelete(deleteId1));
  }

  @Test
  public void replayEvents() throws Exception {
    // start standby masters
    TestJournalSink standbySink = new TestJournalSink(new LinkedBlockingQueue<>());
    startMasters(new MasterRegistry(), JournalTestUtils.createJournalSystem(mJournalFolder),
        standbySink, false);

    int nextCreateId1 = 1;
    int nextCreateId2 = 1;
    int nextRenameId = 1;
    int nextDeleteId = 1;
    int completed = 0;
    while (completed < 5000) {
      switch (ThreadLocalRandom.current().nextInt(4)) {
        case 0:
          createFile("/file_for_rename" + nextCreateId1++);
          completed++;
          break;
        case 1:
          createFile("/file_for_delete" + nextCreateId2++);
          completed++;
          break;
        case 2:
          if (nextRenameId < nextCreateId1) {
            mFileSystemMaster.rename(new AlluxioURI("/file_for_rename" + nextRenameId),
                new AlluxioURI("/renamed" + nextRenameId), RenameContext.defaults());
            nextRenameId++;
            completed++;
          }
          break;
        case 3:
          if (nextDeleteId < nextCreateId2) {
            mFileSystemMaster.delete(new AlluxioURI("/file_for_delete" + nextDeleteId),
                DeleteContext.create(DeletePOptions.newBuilder().setRecursive(true)));
            nextDeleteId++;
            completed++;
          }
          break;
        default:
      }
    }

    // Stop and restart the default masters
    stopMasters(mRegistry, mJournalSystem);
    mJournalSystem.removeJournalSink(mRegistry.get(FileSystemMaster.class), mJournalSink);
    TestJournalSink restartSink = new TestJournalSink(new LinkedBlockingQueue<>());
    startMasters(mRegistry, mJournalSystem, restartSink, true);

    CommonUtils.waitFor("leader receives all entries",
        () -> mJournalSink.getEntries().size() == restartSink.getEntries().size(),
        WaitForOptions.defaults().setTimeoutMs(5000));

    CommonUtils.waitFor("standby receives all entries",
        () -> mJournalSink.getEntries().size() == standbySink.getEntries().size(),
        WaitForOptions.defaults().setTimeoutMs(5000));

    // Need to strip out the sequence number because
    List<JournalEntry> masterEntries = stripSeqNo(mJournalSink.getEntries());
    List<JournalEntry> restartEntries = stripSeqNo(restartSink.getEntries());
    List<JournalEntry> standbyEntries = stripSeqNo(standbySink.getEntries());

    assertFalse(masterEntries.isEmpty());
    assertEquals("leader restart failed to see all entries", masterEntries,
        restartEntries);
    assertEquals("standby failed to see all entries", masterEntries,
        standbyEntries);
  }

  @Test
  public void writeInodePaths() throws Exception {
    String path = "/file";
    createFile(path);
    final long createId1 = findFile("file", path);
    assertNotEquals(INVALID_ID, createId1);
    assertNotEquals(INVALID_ID, findCompleteFile(createId1, path));

    path = "/for_nested_file/nested_file";
    createFile(path);
    assertNotEquals(INVALID_ID, findDir("for_nested_file",
        new AlluxioURI(path).getParent().getPath()));
    final long createId2 = findFile("nested_file", path);
    assertNotEquals(INVALID_ID, createId2);
    assertNotEquals(INVALID_ID, findCompleteFile(createId2, path));

    path = "/dir";
    createDir(path);
    assertNotEquals(INVALID_ID, findDir("dir", path));

    path = "/for_nested_dir/nested_dir";
    createDir(path);
    assertNotEquals(INVALID_ID, findDir("for_nested_dir",
        new AlluxioURI(path).getParent().getPath()));
    assertNotEquals(INVALID_ID, findDir("nested_dir", path));

    path = "/for_nested_dir/nested_dir/nested_dir_2";
    createDir(path);
    assertNotEquals(INVALID_ID, findDir("nested_dir_2", path));

    path = "/for_nested_dir/nested_dir_3/nested_dir_4";
    createDir(path);
    assertNotEquals(INVALID_ID, findDir("nested_dir_3",
        new AlluxioURI(path).getParent().getPath()));
    assertNotEquals(INVALID_ID, findDir("nested_dir_4", path));

    path = "/rename_src";
    String newPath = "/rename_dst";
    createFile(path);
    mFileSystemMaster.rename(new AlluxioURI(path), new AlluxioURI(newPath),
        RenameContext.defaults());
    final long renameId = findFile("rename_src", path);
    assertNotEquals(INVALID_ID, renameId);
    assertNotEquals(INVALID_ID, findCompleteFile(renameId, path));
    assertNotEquals(INVALID_ID, findRename("rename_dst", path, newPath));

    path = "/deleted_file";
    createFile(path);
    mFileSystemMaster.delete(new AlluxioURI(path), DeleteContext.defaults());
    final long deleteId = findFile("deleted_file", path);
    assertNotEquals(INVALID_ID, deleteId);
    assertNotEquals(INVALID_ID, findCompleteFile(deleteId, path));
    assertNotEquals(INVALID_ID, findDelete(deleteId, path));

    path = "/deleted_dir/file1";
    createFile(path);
    mFileSystemMaster.delete(new AlluxioURI("/deleted_dir"),
        DeleteContext.create(DeletePOptions.newBuilder().setRecursive(true)));
    final long deleteId1 = findFile("file1", path);
    assertNotEquals(INVALID_ID, deleteId1);
    assertNotEquals(INVALID_ID, findCompleteFile(deleteId1, path));
    assertNotEquals(INVALID_ID, findDelete(deleteId1, path));
  }

  private long findFile(String name) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasInodeFile() && entry.getInodeFile().getName().equals(name)) {
        return entry.getInodeFile().getId();
      }
    }
    return INVALID_ID;
  }

  private long findFile(String name, String path) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasInodeFile() && entry.getInodeFile().getName().equals(name)
          && entry.getInodeFile().getPath().equals(path)) {
        return entry.getInodeFile().getId();
      }
    }
    return INVALID_ID;
  }

  private long findDir(String name) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasInodeDirectory() && entry.getInodeDirectory().getName().equals(name)) {
        return entry.getInodeDirectory().getId();
      }
    }
    return INVALID_ID;
  }

  private long findDir(String name, String path) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasInodeDirectory() && entry.getInodeDirectory().getName().equals(name)
          && entry.getInodeDirectory().getPath().equals(path)) {
        return entry.getInodeDirectory().getId();
      }
    }
    return INVALID_ID;
  }

  private long findRename(String newName) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasRename() && entry.getRename().getNewName().equals(newName)) {
        return entry.getRename().getId();
      }
    }
    return INVALID_ID;
  }

  private long findRename(String newName, String path, String newPath) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasRename() && entry.getRename().getNewName().equals(newName)
          && entry.getRename().getNewPath().equals(newPath)
          && entry.getRename().getPath().equals(path)) {
        return entry.getRename().getId();
      }
    }
    return INVALID_ID;
  }

  private long findCompleteFile(long id, String path) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasUpdateInodeFile() && entry.getUpdateInodeFile().getId() == id
          && entry.getUpdateInodeFile().getCompleted()
          && entry.getUpdateInodeFile().getPath().equals(path)) {
        return entry.getUpdateInodeFile().getId();
      }
    }
    return INVALID_ID;
  }

  private long findDelete(long id) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasDeleteFile() && entry.getDeleteFile().getId() == id) {
        return entry.getDeleteFile().getId();
      }
    }
    return INVALID_ID;
  }

  private long findDelete(long id, String path) throws Exception {
    while (!mEntries.isEmpty()) {
      JournalEntry entry = mEntries.poll();
      if (entry.hasDeleteFile() && entry.getDeleteFile().getId() == id
          && entry.getDeleteFile().getPath().equals(path)) {
        return entry.getDeleteFile().getId();
      }
    }
    return INVALID_ID;
  }

  private void createFile(String path) throws Exception {
    CreateFileContext createFileContext = CreateFileContext
        .create(CreateFilePOptions.newBuilder().setRecursive(true).setBlockSizeBytes(1024));
    mFileSystemMaster.createFile(new AlluxioURI(path), createFileContext);
    mFileSystemMaster.completeFile(new AlluxioURI(path), CompleteFileContext.defaults());
  }

  private void createDir(String path) throws Exception {
    CreateDirectoryContext createDirContext =
        CreateDirectoryContext.create(CreateDirectoryPOptions.newBuilder().setRecursive(true));
    mFileSystemMaster.createDirectory(new AlluxioURI(path), createDirContext);
  }

  private List<JournalEntry> stripSeqNo(List<JournalEntry> entries) {
    List<JournalEntry> stripped = new ArrayList<>();
    for (JournalEntry e : entries) {
      stripped.add(e.toBuilder().clearSequenceNumber().build());
    }
    return stripped;
  }

  private void startMasters(MasterRegistry registry, JournalSystem journalSystem, JournalSink sink,
      boolean leader) throws Exception {
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(journalSystem);

    new MetricsMasterFactory().create(registry, masterContext);
    new BlockMasterFactory().create(registry, masterContext);
    new FileSystemMasterFactory().create(registry, masterContext);

    if (sink != null) {
      // add journal sink
      journalSystem.addJournalSink(registry.get(FileSystemMaster.class), sink);
    }

    journalSystem.start();
    if (leader) {
      journalSystem.gainPrimacy();
    }
    registry.start(leader);
  }

  private void stopMasters(MasterRegistry registry, JournalSystem journalSystem) throws Exception {
    registry.stop();
    journalSystem.stop();
  }

  private class TestJournalSink implements JournalSink {
    private final List<JournalEntry> mAllEntries;
    private final Queue<JournalEntry> mQueue;

    TestJournalSink(Queue<JournalEntry> queue) {
      mQueue = queue;
      mAllEntries = new ArrayList<>();
    }

    public List<JournalEntry> getEntries() {
      return mAllEntries;
    }

    @Override
    public void append(JournalEntry entry) {
      mQueue.add(entry);
      mAllEntries.add(entry);
    }

    @Override
    public void flush() {
    }
  }
}
