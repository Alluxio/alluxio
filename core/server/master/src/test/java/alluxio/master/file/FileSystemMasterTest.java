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

package alluxio.master.file;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedClientUserResource;
import alluxio.AuthenticatedUserRule;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.StorageList;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.ExistsContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.ScheduleAsyncPersistenceContext;
import alluxio.master.file.contexts.SetAclContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.contexts.WorkerHeartbeatContext;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.Journal;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.util.IdUtils;
import alluxio.util.io.FileUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.FileSystemCommand;
import alluxio.wire.UfsInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
@RunWith(Parameterized.class)
public final class FileSystemMasterTest extends FileSystemMasterTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMasterTest.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {new ImmutableMap.Builder<PropertyKey, Object>()
            .put(PropertyKey.MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS, false)
            .build()},
        {new ImmutableMap.Builder<PropertyKey, Object>()
            .put(PropertyKey.MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS, true)
            .put(PropertyKey.MASTER_RECURSIVE_OPERATION_JOURNAL_FORCE_FLUSH_MAX_ENTRIES, 0)
            .build()},
    });
  }

  @Parameterized.Parameter
  public ImmutableMap<PropertyKey, Object> mConfigMap;

  @Override
  public void before() throws Exception {
    for (Map.Entry<PropertyKey, Object> entry : mConfigMap.entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }
    super.before();
  }

  @Test
  public void createPathWithWhiteSpaces() throws Exception {
    String[] paths = new String[]{
        "/ ",
        "/  ",
        "/ path",
        "/path ",
        "/pa th",
        "/ pa th ",
        "/pa/ th",
        "/pa / th",
        "/ pa / th ",
    };
    for (String path : paths) {
      AlluxioURI uri = new AlluxioURI(path);
      long id = mFileSystemMaster.createFile(uri, CreateFileContext.mergeFrom(
          CreateFilePOptions.newBuilder().setRecursive(true))).getFileId();
      Assert.assertEquals(id, mFileSystemMaster.getFileId(uri));
      mFileSystemMaster.delete(uri, DeleteContext.defaults());
      id = mFileSystemMaster.createDirectory(uri, CreateDirectoryContext
          .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
      Assert.assertEquals(id, mFileSystemMaster.getFileId(uri));
    }
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory with persistent entries with a sync check.
   */
  @Test
  public void deleteSyncedPersistedDirectoryWithCheck() throws Exception {
    deleteSyncedPersistedDirectory(1, false);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory with persistent entries without a sync check.
   */
  @Test
  public void deleteSyncedPersistedDirectoryWithoutCheck() throws Exception {
    deleteSyncedPersistedDirectory(1, true);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a multi-level directory with persistent entries with a sync check.
   */
  @Test
  public void deleteSyncedPersistedMultilevelDirectoryWithCheck() throws Exception {
    deleteSyncedPersistedDirectory(3, false);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a multi-level directory with persistent entries without a sync check.
   */
  @Test
  public void deleteSyncedPersistedMultilevelDirectoryWithoutCheck() throws Exception {
    deleteSyncedPersistedDirectory(3, true);
  }

  // Helper method for deleteSynctedPersisted* tests.
  private void deleteSyncedPersistedDirectory(int levels, boolean unchecked) throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(levels);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(levels);
    // delete top-level directory
    mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)
            .setAlluxioOnly(false).setUnchecked(unchecked)));
    checkPersistedDirectoriesDeleted(levels, ufsMount, Collections.EMPTY_LIST);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory with un-synced persistent entries with a sync check.
   */
  @Test
  public void deleteUnsyncedPersistedDirectoryWithCheck() throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(1);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(1);
    // Add a file to the UFS.
    Files.createFile(
        Paths.get(ufsMount.join(DIR_TOP_LEVEL).join(FILE_PREFIX + (DIR_WIDTH)).getPath()));
    // delete top-level directory
    try {
      mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)
              .setAlluxioOnly(false).setUnchecked(false)));
      fail();
    } catch (IOException e) {
      // Expected
    }
    // Check all that could be deleted.
    List<AlluxioURI> except = new ArrayList<>();
    except.add(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL));
    checkPersistedDirectoriesDeleted(1, ufsMount, except);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory with un-synced persistent entries without a sync check.
   */
  @Test
  public void deleteUnsyncedPersistedDirectoryWithoutCheck() throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(1);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(1);
    // Add a file to the UFS.
    Files.createFile(
        Paths.get(ufsMount.join(DIR_TOP_LEVEL).join(FILE_PREFIX + (DIR_WIDTH)).getPath()));
    // delete top-level directory
    mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL), DeleteContext.mergeFrom(
        DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).setUnchecked(true)));
    checkPersistedDirectoriesDeleted(1, ufsMount, Collections.EMPTY_LIST);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a multi-level directory with un-synced persistent entries with a sync check.
   */
  @Test
  public void deleteUnsyncedPersistedMultilevelDirectoryWithCheck() throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(3);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(3);
    // Add a file to the UFS down the tree.
    Files.createFile(Paths.get(ufsMount.join(DIR_TOP_LEVEL).join(DIR_PREFIX + 0)
        .join(FILE_PREFIX + (DIR_WIDTH)).getPath()));
    mThrown.expect(IOException.class);
    // delete top-level directory
    mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL), DeleteContext.mergeFrom(
        DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).setUnchecked(false)));
    // Check all that could be deleted.
    List<AlluxioURI> except = new ArrayList<>();
    except.add(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL));
    except.add(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL).join(DIR_PREFIX + 0));
    checkPersistedDirectoriesDeleted(3, ufsMount, except);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a multi-level directory with un-synced persistent entries without a sync check.
   */
  @Test
  public void deleteUnsyncedPersistedMultilevelDirectoryWithoutCheck() throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(3);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(3);
    // Add a file to the UFS down the tree.
    Files.createFile(Paths.get(ufsMount.join(DIR_TOP_LEVEL).join(DIR_PREFIX + 0)
        .join(FILE_PREFIX + (DIR_WIDTH)).getPath()));
    // delete top-level directory
    mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL), DeleteContext.mergeFrom(
        DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).setUnchecked(true)));
    checkPersistedDirectoriesDeleted(3, ufsMount, Collections.EMPTY_LIST);
  }

  /**
   * Tests the {@link FileSystemMaster#getNewBlockIdForFile(AlluxioURI)} method.
   */
  @Test
  public void getNewBlockIdForFile() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());
  }

  @Test
  public void getPath() throws Exception {
    AlluxioURI rootUri = new AlluxioURI("/");
    long rootId = mFileSystemMaster.getFileId(rootUri);
    assertEquals(rootUri, mFileSystemMaster.getPath(rootId));

    // get non-existent id
    try {
      mFileSystemMaster.getPath(rootId + 1234);
      fail("getPath() for a non-existent id should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  /**
   * Tests the {@link FileSystemMaster#getPersistenceState(long)} method.
   */
  @Test
  public void getPersistenceState() throws Exception {
    AlluxioURI rootUri = new AlluxioURI("/");
    long rootId = mFileSystemMaster.getFileId(rootUri);
    assertEquals(PersistenceState.PERSISTED, mFileSystemMaster.getPersistenceState(rootId));

    // get non-existent id
    try {
      mFileSystemMaster.getPersistenceState(rootId + 1234);
      fail("getPath() for a non-existent id should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  /**
   * Tests the {@link FileSystemMaster#getFileId(AlluxioURI)} method.
   */
  @Test
  public void getFileId() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);

    // These URIs exist.
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertEquals(ROOT_URI, mFileSystemMaster.getPath(mFileSystemMaster.getFileId(ROOT_URI)));

    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertEquals(NESTED_URI,
        mFileSystemMaster.getPath(mFileSystemMaster.getFileId(NESTED_URI)));

    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    assertEquals(NESTED_FILE_URI,
        mFileSystemMaster.getPath(mFileSystemMaster.getFileId(NESTED_FILE_URI)));

    // These URIs do not exist.
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_FILE_URI));
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(TEST_URI));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(NESTED_FILE_URI.join("DNE")));
  }

  @Test
  public void getFileBlockInfoList() throws Exception {
    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);

    List<FileBlockInfo> blockInfo;

    blockInfo = mFileSystemMaster.getFileBlockInfoList(ROOT_FILE_URI);
    assertEquals(1, blockInfo.size());

    blockInfo = mFileSystemMaster.getFileBlockInfoList(NESTED_FILE_URI);
    assertEquals(1, blockInfo.size());

    // Test directory URI.
    try {
      mFileSystemMaster.getFileBlockInfoList(NESTED_URI);
      fail("getFileBlockInfoList() for a directory URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    // Test non-existent URI.
    try {
      mFileSystemMaster.getFileBlockInfoList(TEST_URI);
      fail("getFileBlockInfoList() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void mountUnmount() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Alluxio mount point should not exist before mounting.
    try {
      mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local"), GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI (before mounting) should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());
    // Alluxio mount point should exist after mounting.
    assertNotNull(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local"), GET_STATUS_CONTEXT));

    mFileSystemMaster.unmount(new AlluxioURI("/mnt/local"));

    // Alluxio mount point should not exist after unmounting.
    try {
      mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local"), GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI (after mounting) should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void inheritExtendedDefaultAcl() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    mFileSystemMaster.createDirectory(dir, CreateDirectoryContext.defaults());
    String aclString = "default:user:foo:-w-";
    mFileSystemMaster.setAcl(dir, SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString(aclString)), SetAclContext.defaults());
    AlluxioURI inner = new AlluxioURI("/dir/inner");
    mFileSystemMaster.createDirectory(inner, CreateDirectoryContext.defaults());
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(inner, GetStatusContext.defaults());
    List<String> accessEntries = fileInfo.getAcl().toStringEntries();
    assertTrue(accessEntries.toString(), accessEntries.contains("user:foo:-w-"));
    List<String> defaultEntries = fileInfo.getDefaultAcl().toStringEntries();
    assertTrue(defaultEntries.toString(), defaultEntries.contains(aclString));
  }

  @Test
  public void inheritNonExtendedDefaultAcl() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    mFileSystemMaster.createDirectory(dir, CreateDirectoryContext.defaults());
    String aclString = "default:user::-w-";
    mFileSystemMaster.setAcl(dir, SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString(aclString)), SetAclContext.defaults());
    AlluxioURI inner = new AlluxioURI("/dir/inner");
    mFileSystemMaster.createDirectory(inner, CreateDirectoryContext.defaults());
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(inner, GetStatusContext.defaults());
    List<String> accessEntries = fileInfo.getAcl().toStringEntries();
    assertTrue(accessEntries.toString(), accessEntries.contains("user::-w-"));
    List<String> defaultEntries = fileInfo.getDefaultAcl().toStringEntries();
    assertTrue(defaultEntries.toString(), defaultEntries.contains(aclString));
  }

  @Test
  public void setAclWithoutOwner() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    Set<String> entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(3, entries.size());

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      Set<String> newEntries = Sets.newHashSet("user::rwx", "group::rwx", "other::rwx");
      mThrown.expect(AccessControlException.class);
      mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.REPLACE,
          newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()),
          SetAclContext.defaults());
    }
  }

  @Test
  public void setAclNestedWithoutOwner() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext.mergeFrom(SetAttributePOptions
        .newBuilder().setMode(new Mode((short) 0777).toProto()).setOwner("userA")));
    Set<String> entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(3, entries.size());
    // recursive setAcl should fail if one of the child is not owned by the user
    mThrown.expect(AccessControlException.class);
    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      Set<String> newEntries = Sets.newHashSet("user::rwx", "group::rwx", "other::rwx");
      mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.REPLACE,
          newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()),
          SetAclContext.mergeFrom(SetAclPOptions.newBuilder().setRecursive(true)));
      entries = Sets.newHashSet(mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT)
          .convertAclToStringEntries());
      assertEquals(newEntries, entries);
    }
  }

  @Test
  public void removeExtendedAclMask() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    AclEntry newAcl = AclEntry.fromCliString("user:newuser:rwx");
    // Add an ACL
    addAcl(NESTED_URI, newAcl);
    assertThat(getInfo(NESTED_URI).getAcl().getEntries(), hasItem(newAcl));

    // Attempt to remove the ACL mask
    AclEntry maskEntry = AclEntry.fromCliString("mask::rwx");
    assertThat(getInfo(NESTED_URI).getAcl().getEntries(), hasItem(maskEntry));
    try {
      removeAcl(NESTED_URI, maskEntry);
      fail("Expected removing the mask from an extended ACL to fail");
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("mask"));
    }

    // Remove the extended ACL
    removeAcl(NESTED_URI, newAcl);
    // Now we can add and remove a mask
    addAcl(NESTED_URI, maskEntry);
    removeAcl(NESTED_URI, maskEntry);
  }

  @Test
  public void removeExtendedDefaultAclMask() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    AclEntry newAcl = AclEntry.fromCliString("default:user:newuser:rwx");
    // Add an ACL
    addAcl(NESTED_URI, newAcl);
    assertThat(getInfo(NESTED_URI).getDefaultAcl().getEntries(), hasItem(newAcl));

    // Attempt to remove the ACL mask
    AclEntry maskEntry = AclEntry.fromCliString("default:mask::rwx");
    assertThat(getInfo(NESTED_URI).getDefaultAcl().getEntries(), hasItem(maskEntry));
    try {
      removeAcl(NESTED_URI, maskEntry);
      fail("Expected removing the mask from an extended ACL to fail");
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("mask"));
    }

    // Remove the extended ACL
    removeAcl(NESTED_URI, newAcl);
    // Now we can add and remove a mask
    addAcl(NESTED_URI, maskEntry);
    removeAcl(NESTED_URI, maskEntry);
  }

  private void addAcl(AlluxioURI uri, AclEntry acl) throws Exception {
    mFileSystemMaster.setAcl(uri, SetAclAction.MODIFY, Arrays.asList(acl),
        SetAclContext.defaults());
  }

  private void removeAcl(AlluxioURI uri, AclEntry acl) throws Exception {
    mFileSystemMaster.setAcl(uri, SetAclAction.REMOVE, Arrays.asList(acl),
        SetAclContext.defaults());
  }

  private FileInfo getInfo(AlluxioURI uri) throws Exception {
    return mFileSystemMaster.getFileInfo(uri, GetStatusContext.defaults());
  }

  /**
   * Tests that an exception is in the
   * {@link FileSystemMaster#createFile(AlluxioURI, CreateFileContext)} with a
   * TTL set in the {@link CreateFileContext} after the TTL check was done once.
   */
  @Test
  public void ttlFileDelete() throws Exception {
    CreateFileContext context = CreateFileContext.defaults();
    context.getOptions().setBlockSizeBytes(Constants.KB);
    context.getOptions().setRecursive(true);
    context.getOptions().setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(fileInfo.getFileId(), fileId);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that TTL delete of a file is not forgotten across restarts.
   */
  @Test
  public void ttlFileDeleteReplay() throws Exception {
    CreateFileContext context = CreateFileContext.defaults();
    context.getOptions().setBlockSizeBytes(Constants.KB);
    context.getOptions().setRecursive(true);
    context.getOptions().setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();

    // Simulate restart.
    stopServices();
    startServices();

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(fileInfo.getFileId(), fileId);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is in the
   * {@literal FileSystemMaster#createDirectory(AlluxioURI, CreateDirectoryOptions)}
   * with a TTL set in the {@link CreateDirectoryContext} after the TTL check was done once.
   */
  @Test
  public void ttlDirectoryDelete() throws Exception {
    CreateDirectoryContext context =
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0)));
    long dirId = mFileSystemMaster.createDirectory(NESTED_DIR_URI, context);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(dirId);
    assertEquals(fileInfo.getFileId(), dirId);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(dirId);
  }

  /**
   * Tests that TTL delete of a directory is not forgotten across restarts.
   */
  @Test
  public void ttlDirectoryDeleteReplay() throws Exception {
    CreateDirectoryContext context =
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0)));
    long dirId = mFileSystemMaster.createDirectory(NESTED_DIR_URI, context);

    // Simulate restart.
    stopServices();
    startServices();

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(dirId);
    assertEquals(fileInfo.getFileId(), dirId);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(dirId);
  }

  /**
   * Tests that file information is still present after it has been freed after the TTL has been set
   * to 0.
   */
  @Test
  public void ttlFileFree() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext.mergeFrom(
        SetAttributePOptions.newBuilder().setCommonOptions(FileSystemOptionsUtils
            .commonDefaults(Configuration.global()).toBuilder().setTtl(0)
            .setTtlAction(alluxio.grpc.TtlAction.FREE))));
    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that TTL free of a file is not forgotten across restarts.
   */
  @Test
  public void ttlFileFreeReplay() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext.mergeFrom(
        SetAttributePOptions.newBuilder().setCommonOptions(FileSystemOptionsUtils
            .commonDefaults(Configuration.global()).toBuilder().setTtl(0)
            .setTtlAction(alluxio.grpc.TtlAction.FREE))));
    // Simulate restart.
    stopServices();
    startServices();

    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.<String, StorageList>of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that file information is still present after it has been freed after the parent
   * directory's TTL has been set to 0.
   */
  @Test
  public void ttlDirectoryFree() throws Exception {
    CreateDirectoryContext directoryContext = CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
    mFileSystemMaster.createDirectory(NESTED_URI, directoryContext);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation.
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext.mergeFrom(
        SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(0).setTtlAction(alluxio.grpc.TtlAction.FREE))));
    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.<String, StorageList>of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that TTL free of a directory is not forgotten across restarts.
   */
  @Test
  public void ttlDirectoryFreeReplay() throws Exception {
    CreateDirectoryContext directoryContext = CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
    mFileSystemMaster.createDirectory(NESTED_URI, directoryContext);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation.
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext.mergeFrom(
        SetAttributePOptions.newBuilder().setCommonOptions(FileSystemOptionsUtils
            .commonDefaults(Configuration.global()).toBuilder().setTtl(0)
            .setTtlAction(alluxio.grpc.TtlAction.FREE))));

    // Simulate restart.
    stopServices();
    startServices();

    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that an exception is thrown when trying to get information about a file after it
   * has been deleted because of a TTL of 0.
   */
  @Test
  public void setTtlForFileWithNoTtl() throws Exception {
    CreateFileContext context = CreateFileContext.mergeFrom(
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since no TTL is set, the file should not be deleted.
    assertEquals(fileId,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is set to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a Directory after
   * it has been deleted because of a TTL of 0.
   */
  @Test
  public void setTtlForDirectoryWithNoTtl() throws Exception {
    CreateDirectoryContext directoryContext = CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
    mFileSystemMaster.createDirectory(NESTED_URI, directoryContext);
    mFileSystemMaster.createDirectory(NESTED_DIR_URI, directoryContext);
    CreateFileContext createFileContext = CreateFileContext.mergeFrom(
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, createFileContext).getFileId();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since no TTL is set, the file should not be deleted.
    assertEquals(fileId,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getFileId());
    // Set ttl.
    mFileSystemMaster.setAttribute(NESTED_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is set to 0, the file and directory should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT);
    mFileSystemMaster.getFileInfo(NESTED_DIR_URI, GET_STATUS_CONTEXT);
    mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a file after it
   * has been deleted after the TTL has been set to 0.
   */
  @Test
  public void setSmallerTtlForFileWithTtl() throws Exception {
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.HOUR_MS))
        .setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since TTL is 1 hour, the file won't be deleted during last TTL check.
    assertEquals(fileId,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a Directory after
   * it has been deleted after the TTL has been set to 0.
   */
  @Test
  public void setSmallerTtlForDirectoryWithTtl() throws Exception {
    CreateDirectoryContext directoryContext =
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.HOUR_MS))
            .setRecursive(true));
    mFileSystemMaster.createDirectory(NESTED_URI, directoryContext);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    assertTrue(
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getName() != null);
    mFileSystemMaster.setAttribute(NESTED_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT);
  }

  /**
   * Tests that a file has not been deleted after the TTL has been reset to a valid value.
   */
  @Test
  public void setLargerTtlForFileWithTtl() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))
        .setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    assertEquals(fileId,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(Constants.HOUR_MS))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 1 hour, the file should not be deleted during last TTL check.
    assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests that a directory has not been deleted after the TTL has been reset to a valid value.
   */
  @Test
  public void setLargerTtlForDirectoryWithTtl() throws Exception {
    mFileSystemMaster.createDirectory(new AlluxioURI("/nested"),
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    mFileSystemMaster.createDirectory(NESTED_URI,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))
            .setRecursive(true)));
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(Constants.HOUR_MS))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 1 hour, the directory should not be deleted during last TTL check.
    assertEquals(NESTED_URI.getName(),
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getName());
  }

  /**
   * Tests that the original TTL is removed after setting it to {@link Constants#NO_TTL} for a file.
   */
  @Test
  public void setNoTtlForFileWithTtl() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))
        .setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(Constants.NO_TTL))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests that the original TTL is removed after setting it to {@link Constants#NO_TTL} for
   * a directory.
   */
  @Test
  public void setNoTtlForDirectoryWithTtl() throws Exception {
    mFileSystemMaster.createDirectory(new AlluxioURI("/nested"),
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    mFileSystemMaster.createDirectory(NESTED_URI,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))
            .setRecursive(true)));
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(Constants.NO_TTL))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    assertEquals(NESTED_URI.getName(),
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getName());
  }

  /**
   * Tests the {@link FileSystemMaster#setAttribute(AlluxioURI, SetAttributeContext)} method and
   * that an exception is thrown when trying to set a TTL for a directory.
   */
  @Test
  public void setAttribute() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertFalse(fileInfo.isPinned());
    assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // No State.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext.defaults());
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertFalse(fileInfo.isPinned());
    assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Just set pinned flag.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertTrue(fileInfo.isPinned());
    assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Both pinned flag and ttl value.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(false)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                .setTtl(1))));
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertFalse(fileInfo.isPinned());
    assertEquals(1, fileInfo.getTtl());

    mFileSystemMaster.setAttribute(NESTED_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                .setTtl(1))));
  }

  /**
   * Tests the permission bits are 0777 for directories and 0666 for files with UMASK 000.
   */
  @Test
  public void permission() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);
    assertEquals(0777,
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getMode());
    assertEquals(0666,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getMode());
  }

  /**
   * Tests that a file is fully written to memory.
   */
  @Test
  public void isFullyInMemory() throws Exception {
    // add nested file
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);
    // add in-memory block
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB,
        Constants.MEDIUM_MEM, Constants.MEDIUM_MEM, blockId, Constants.KB);
    // add SSD block
    blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, Constants.MEDIUM_SSD,
        Constants.MEDIUM_SSD, blockId, Constants.KB);
    mFileSystemMaster.completeFile(NESTED_FILE_URI, CompleteFileContext.defaults());

    // Create 2 files in memory.
    createFileWithSingleBlock(ROOT_FILE_URI);
    AlluxioURI nestedMemUri = NESTED_URI.join("mem_file");
    createFileWithSingleBlock(nestedMemUri);
    assertEquals(2, mFileSystemMaster.getInMemoryFiles().size());
    assertTrue(mFileSystemMaster.getInMemoryFiles().contains(ROOT_FILE_URI));
    assertTrue(mFileSystemMaster.getInMemoryFiles().contains(nestedMemUri));
  }

  /**
   * Tests {@link FileSystemMaster#free} on persisted file.
   */
  @Test
  public void free() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the file
    mFileSystemMaster.free(NESTED_FILE_URI, FreeContext.mergeFrom(FreePOptions.newBuilder()
        .setForced(false).setRecursive(false)));
    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat2 = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat2);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests {@link FileSystemMaster#free} on non-persisted file.
   */
  @Test
  public void freeNonPersistedFile() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_NON_PERSISTED_FILE
        .getMessage(NESTED_FILE_URI.getPath()));
    // cannot free a non-persisted file
    mFileSystemMaster.free(NESTED_FILE_URI, FreeContext.defaults());
  }

  /**
   * Tests {@link FileSystemMaster#free} on pinned file when forced flag is false.
   */
  @Test
  public void freePinnedFileWithoutForce() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_PINNED_FILE
        .getMessage(NESTED_FILE_URI.getPath()));
    // cannot free a pinned file without "forced"
    mFileSystemMaster.free(NESTED_FILE_URI, FreeContext.defaults());
  }

  /**
   * Tests {@link FileSystemMaster#free} on pinned file when forced flag is true.
   */
  @Test
  public void freePinnedFileWithForce() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));

    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the file
    mFileSystemMaster.free(NESTED_FILE_URI,
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true)));
    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory but recursive to false.
   */
  @Test
  public void freeDirNonRecursive() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    createFileWithSingleBlock(NESTED_FILE_URI);
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_NON_EMPTY_DIR.getMessage(NESTED_URI));
    // cannot free directory with recursive argument to false
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setRecursive(false)));
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory.
   */
  @Test
  public void freeDir() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the dir
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true).setRecursive(true)));
    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat3 = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat3);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory with a file non-persisted.
   */
  @Test
  public void freeDirWithNonPersistedFile() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_NON_PERSISTED_FILE
        .getMessage(NESTED_FILE_URI.getPath()));
    // cannot free the parent dir of a non-persisted file
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(false).setRecursive(true)));
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory with a file pinned when
   * forced flag is false.
   */
  @Test
  public void freeDirWithPinnedFileAndNotForced() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_PINNED_FILE
        .getMessage(NESTED_FILE_URI.getPath()));
    // cannot free the parent dir of a pinned file without "forced"
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(false).setRecursive(true)));
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory with a file pinned when
   * forced flag is true.
   */
  @Test
  public void freeDirWithPinnedFileAndForced() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));
    // free the parent dir of a pinned file with "forced"
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true).setRecursive(true)));
    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#mount(AlluxioURI, AlluxioURI, MountContext)} method.
   */
  @Test
  public void mount() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
  }

  /**
   * Tests mounting an existing dir.
   */
  @Test
  public void mountExistingDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.createDirectory(alluxioURI, CreateDirectoryContext.defaults());
    mThrown.expect(InvalidPathException.class);
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
  }

  /**
   * Tests mounting to an Alluxio path whose parent dir does not exist.
   */
  @Test
  public void mountNonExistingParentDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/non-existing/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
  }

  /**
   * Tests a readOnly mount for the create directory op.
   */
  @Test
  public void mountReadOnlyCreateDirectory() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI path = new AlluxioURI("/hello/dir1");
    mFileSystemMaster.createDirectory(path, CreateDirectoryContext.defaults());
  }

  /**
   * Tests a readOnly mount for the create file op.
   */
  @Test
  public void mountReadOnlyCreateFile() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI path = new AlluxioURI("/hello/file1");
    mFileSystemMaster.createFile(path, CreateFileContext.defaults());
  }

  /**
   * Tests a readOnly mount for the delete op.
   */
  @Test
  public void mountReadOnlyDeleteFile() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    createTempUfsFile("ufs/hello/file1");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI path = new AlluxioURI("/hello/file1");
    mFileSystemMaster.delete(path, DeleteContext.defaults());
  }

  /**
   * Tests a readOnly mount for the rename op.
   */
  @Test
  public void mountReadOnlyRenameFile() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    createTempUfsFile("ufs/hello/file1");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI src = new AlluxioURI("/hello/file1");
    AlluxioURI dst = new AlluxioURI("/hello/file2");
    mFileSystemMaster.rename(src, dst, RenameContext.defaults());
  }

  /**
   * Tests a readOnly mount for the set attribute op.
   */
  @Test
  public void mountReadOnlySetAttribute() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    createTempUfsFile("ufs/hello/file1");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI path = new AlluxioURI("/hello/file1");
    mFileSystemMaster.setAttribute(path,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setOwner("owner")));
  }

  /**
   * Tests mounting a shadow Alluxio dir.
   */
  @Test
  public void mountShadowDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello/shadow");

    mFileSystemMaster.mount(alluxioURI, ufsURI.getParent(),
        MountContext.defaults());
    AlluxioURI shadowAlluxioURI = new AlluxioURI("/hello/shadow");
    AlluxioURI notShadowAlluxioURI = new AlluxioURI("/hello/notshadow");
    AlluxioURI shadowUfsURI = createTempUfsDir("ufs/hi");
    AlluxioURI notShadowUfsURI = createTempUfsDir("ufs/notshadowhi");
    mFileSystemMaster.mount(notShadowAlluxioURI, notShadowUfsURI,
        MountContext.defaults());
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.mount(shadowAlluxioURI, shadowUfsURI,
        MountContext.defaults());
  }

  /**
   * Tests mounting a prefix UFS dir.
   */
  @Test
  public void mountPrefixUfsDir() throws Exception {
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello/shadow");
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
    AlluxioURI preUfsURI = ufsURI.getParent();
    AlluxioURI anotherAlluxioURI = new AlluxioURI("/hi");
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.mount(anotherAlluxioURI, preUfsURI, MountContext.defaults());
  }

  /**
   * Tests mounting a suffix UFS dir.
   */
  @Test
  public void mountSuffixUfsDir() throws Exception {
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello/shadow");
    AlluxioURI preUfsURI = ufsURI.getParent();
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.mount(alluxioURI, preUfsURI, MountContext.defaults());
    AlluxioURI anotherAlluxioURI = new AlluxioURI("/hi");
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.mount(anotherAlluxioURI, ufsURI, MountContext.defaults());
  }

  /**
   * Tests unmount operation.
   */
  @Test
  public void unmount() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
    mFileSystemMaster.createDirectory(alluxioURI.join("dir"), CreateDirectoryContext
        .defaults().setWriteType(WriteType.CACHE_THROUGH));
    mFileSystemMaster.unmount(alluxioURI);
    // after unmount, ufs path under previous mount point should still exist
    File file = new File(ufsURI.join("dir").getPath());
    assertTrue(file.exists());
    // after unmount, alluxio path under previous mount point should not exist
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(alluxioURI.join("dir"), GET_STATUS_CONTEXT);
  }

  /**
   * Tests unmount operation failed when unmounting root.
   */
  @Test
  public void unmountRootWithException() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.unmount(new AlluxioURI("/"));
  }

  /**
   * Tests unmount operation failed when unmounting non-mount point.
   */
  @Test
  public void unmountNonMountPointWithException() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
    AlluxioURI dirURI = alluxioURI.join("dir");
    mFileSystemMaster.createDirectory(dirURI, CreateDirectoryContext
        .defaults().setWriteType(WriteType.MUST_CACHE));
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.unmount(dirURI);
  }

  /**
   * Tests unmount operation failed when unmounting non-existing dir.
   */
  @Test
  public void unmountNonExistingPathWithException() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.unmount(new AlluxioURI("/FileNotExists"));
  }

  /**
   * Tests the {@link DefaultFileSystemMaster#stop()} method.
   */
  @Test
  public void stop() throws Exception {
    mRegistry.stop();
    assertTrue(mExecutorService.isShutdown());
    assertTrue(mExecutorService.isTerminated());
  }

  /**
   * Tests the {@link FileSystemMaster#workerHeartbeat} method.
   */
  @Test
  public void workerHeartbeat() throws Exception {
    long blockId = createFileWithSingleBlock(ROOT_FILE_URI);

    long fileId = mFileSystemMaster.getFileId(ROOT_FILE_URI);
    mFileSystemMaster.scheduleAsyncPersistence(ROOT_FILE_URI,
        ScheduleAsyncPersistenceContext.defaults());

    FileSystemCommand command = mFileSystemMaster
        .workerHeartbeat(mWorkerId1, Lists.newArrayList(fileId), WorkerHeartbeatContext.defaults());
    assertEquals(alluxio.wire.CommandType.PERSIST, command.getCommandType());
    assertEquals(0, command.getCommandOptions().getPersistOptions().getFilesToPersist().size());
  }

  /**
   * Tests that lost files can successfully be detected.
   */
  @Test
  public void lostFilesDetection() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long fileId = mFileSystemMaster.getFileId(NESTED_FILE_URI);

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    mBlockMaster.reportLostBlocks(fileInfo.getBlockIds());

    assertEquals(PersistenceState.NOT_PERSISTED.name(), fileInfo.getPersistenceState());
    // Check with getPersistenceState.
    assertEquals(PersistenceState.NOT_PERSISTED,
        mFileSystemMaster.getPersistenceState(fileId));

    // run the detector
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_FILES_DETECTION);

    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(PersistenceState.LOST.name(), fileInfo.getPersistenceState());
    // Check with getPersistenceState.
    assertEquals(PersistenceState.LOST, mFileSystemMaster.getPersistenceState(fileId));
  }

  @Test
  public void getUfsInfo() throws Exception {
    FileInfo alluxioRootInfo =
        mFileSystemMaster.getFileInfo(new AlluxioURI("alluxio://"), GET_STATUS_CONTEXT);
    UfsInfo ufsRootInfo = mFileSystemMaster.getUfsInfo(alluxioRootInfo.getMountId());
    assertEquals(mUnderFS, ufsRootInfo.getUri().getPath());
    assertTrue(ufsRootInfo.getMountOptions().getPropertiesMap().isEmpty());
  }

  @Test
  public void getUfsInfoNotExist() throws Exception {
    UfsInfo noSuchUfsInfo = mFileSystemMaster.getUfsInfo(100L);
    assertNull(noSuchUfsInfo.getUri());
    assertNull(noSuchUfsInfo.getMountOptions());
  }

  /**
   * Tests that setting the ufs fingerprint persists across restarts.
   */
  @Test
  public void setUfsFingerprintReplay() throws Exception {
    String fingerprint = "FINGERPRINT";
    createFileWithSingleBlock(NESTED_FILE_URI);

    ((DefaultFileSystemMaster) mFileSystemMaster).setAttribute(NESTED_FILE_URI,
        SetAttributeContext.defaults().setUfsFingerprint(fingerprint));

    // Simulate restart.
    stopServices();
    startServices();

    assertEquals(fingerprint,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI,
            GetStatusContext.defaults()).getUfsFingerprint());
  }

  @Test
  public void ignoreInvalidFiles() throws Exception {
    FileUtils.createDir(Paths.get(mUnderFS, "test").toString());
    FileUtils.createFile(Paths.get(mUnderFS, "test", "a?b=C").toString());
    FileUtils.createFile(Paths.get(mUnderFS, "test", "valid").toString());
    List<FileInfo> listing = mFileSystemMaster.listStatus(new AlluxioURI("/test"),
        ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    assertEquals(1, listing.size());
    assertEquals("valid", listing.get(0).getName());
  }

  @Test
  public void propagatePersisted() throws Exception {
    AlluxioURI nestedFile = new AlluxioURI("/nested1/nested2/file");
    AlluxioURI parent1 = new AlluxioURI("/nested1/");
    AlluxioURI parent2 = new AlluxioURI("/nested1/nested2/");

    createFileWithSingleBlock(nestedFile);

    // Nothing is persisted yet.
    assertEquals(PersistenceState.NOT_PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(nestedFile, GetStatusContext.defaults())
            .getPersistenceState());
    assertEquals(PersistenceState.NOT_PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent1,
            GetStatusContext.defaults()).getPersistenceState());
    assertEquals(PersistenceState.NOT_PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent2,
            GetStatusContext.defaults()).getPersistenceState());

    // Persist the file.
    mFileSystemMaster.setAttribute(nestedFile,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPersisted(true)));

    // Everything component should be persisted.
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(nestedFile, GetStatusContext.defaults())
            .getPersistenceState());
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent1,
            GetStatusContext.defaults()).getPersistenceState());
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent2,
            GetStatusContext.defaults()).getPersistenceState());

    // Simulate restart.
    stopServices();
    startServices();

    // Everything component should be persisted.
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(nestedFile, GetStatusContext.defaults())
            .getPersistenceState());
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent1,
            GetStatusContext.defaults()).getPersistenceState());
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent2,
            GetStatusContext.defaults()).getPersistenceState());
  }

  /**
   * Tests using SetAttribute to apply xAttr to a file, using the TRUNCATE update strategy.
   */
  @Test
  public void setAttributeXAttrTruncate() throws Exception {
    // Create file with xAttr to overwrite
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("foo", StandardCharsets.UTF_8))
        .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
        .setRecursive(true));
    FileInfo fileInfo = mFileSystemMaster.createFile(ROOT_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "foo");

    // Replace the xAttr map
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("bar", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(alluxio.proto.journal.File.XAttrUpdateStrategy.TRUNCATE)));
    FileInfo updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(updatedFileInfo.getXAttr().size(), 1);
    assertEquals(new String(updatedFileInfo.getXAttr().get("bar"), StandardCharsets.UTF_8),
        "bar");
  }

  /**
   * Tests using SetAttribute to apply xAttr to a file, using the UNION_REPLACE update strategy.
   */
  @Test
  public void setAttributeXAttrUnionReplace() throws Exception {
    // Create file with xAttr to overwrite
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("foo", StandardCharsets.UTF_8))
        .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
        .setRecursive(true));
    FileInfo fileInfo = mFileSystemMaster.createFile(ROOT_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "foo");

    // Combine the xAttr maps, replacing existing keys
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("foo", ByteString.copyFrom("baz", StandardCharsets.UTF_8))
            .putXattr("bar", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(
                alluxio.proto.journal.File.XAttrUpdateStrategy.UNION_REPLACE)));
    FileInfo updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(updatedFileInfo.getXAttr().size(), 2);
    assertEquals(new String(updatedFileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8),
        "baz");
    assertEquals(new String(updatedFileInfo.getXAttr().get("bar"), StandardCharsets.UTF_8),
        "bar");
  }

  /**
   * Tests using SetAttribute to apply xAttr to a file, using the UNION_PRESERVE update strategy.
   */
  @Test
  public void setAttributeXAttrUnionPreserve() throws Exception {
    // Create file with xAttr to overwrite
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("foo", StandardCharsets.UTF_8))
        .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
        .setRecursive(true));
    FileInfo fileInfo = mFileSystemMaster.createFile(ROOT_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "foo");

    // Combine the xAttr maps, preserving existing keys
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("foo", ByteString.copyFrom("baz", StandardCharsets.UTF_8))
            .putXattr("bar", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(
                alluxio.proto.journal.File.XAttrUpdateStrategy.UNION_PRESERVE)));
    FileInfo updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(updatedFileInfo.getXAttr().size(), 2);
    assertEquals(new String(updatedFileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8),
        "foo");
    assertEquals(new String(updatedFileInfo.getXAttr().get("bar"), StandardCharsets.UTF_8),
        "bar");
  }

  /**
   * Tests using SetAttribute to apply xAttr to a file, using the DELETE_KEYS update strategy.
   */
  @Test
  public void setAttributeXAttrDeleteKeys() throws Exception {
    // Create file with xAttr to delete
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("foo", StandardCharsets.UTF_8))
        .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
        .setRecursive(true));
    FileInfo fileInfo = mFileSystemMaster.createFile(ROOT_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "foo");

    // Delete a non-existing key
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("bar", ByteString.copyFrom("", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(alluxio.proto.journal.File.XAttrUpdateStrategy.DELETE_KEYS)));
    FileInfo updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(updatedFileInfo.getXAttr().size(), 1);

    // Delete an existing key
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("foo", ByteString.copyFrom("", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(alluxio.proto.journal.File.XAttrUpdateStrategy.DELETE_KEYS)));
    updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertNullOrEmpty(updatedFileInfo.getXAttr());
  }

  /**
   * Tests setting xAttr when creating a file in a nested directory, and
   * propagates the xAttr back to the parent directories.
   */
  @Test
  public void createFileXAttrPropagateNewPaths() throws Exception {
    createFileXAttr(XAttrPropagationStrategy.NEW_PATHS);
  }

  /**
   * Tests setting xAttr when creating a file in a nested directory, and
   * only assigns xAttr to the leaf node.
   */
  @Test
  public void createFileXAttrPropagateLeaf() throws Exception {
    createFileXAttr(XAttrPropagationStrategy.LEAF_NODE);
  }

  private void createFileXAttr(XAttrPropagationStrategy propStrat) throws Exception {
    // Set xAttr upon creating the file
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
        .setRecursive(true)
        .setXattrPropStrat(propStrat));
    FileInfo fileInfo = mFileSystemMaster.createFile(NESTED_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "bar");

    checkParentXAttrTags(NESTED_FILE_URI, propStrat);
  }

  /**
   * Tests setting xAttr when creating a nested subdirectory, and
   * propagates the xAttr back to the parent directories.
   */
  @Test
  public void createDirXAttrPropagateNewPaths() throws Exception {
    createDirXAttr(XAttrPropagationStrategy.NEW_PATHS);
  }

  /**
   * Tests setting xAttr when creating a nested subdirectory, and
   * only assigns xAttr to the leaf node.
   */
  @Test
  public void createDirXAttrPropagateLeaf() throws Exception {
    createDirXAttr(XAttrPropagationStrategy.LEAF_NODE);
  }

  private void createDirXAttr(XAttrPropagationStrategy propStrat) throws Exception {
    // Set xAttr upon creating the directory
    CreateDirectoryContext directoryContext = CreateDirectoryContext.mergeFrom(
        CreateDirectoryPOptions.newBuilder()
            .putXattr("foo", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
            .setXattrPropStrat(propStrat)
            .setRecursive(true));
    long dirId = mFileSystemMaster.createDirectory(NESTED_DIR_URI, directoryContext);
    assertEquals(new String(mFileSystemMaster.getFileInfo(dirId).getXAttr().get("foo"),
        StandardCharsets.UTF_8), "bar");

    checkParentXAttrTags(NESTED_DIR_URI, propStrat);
  }

  private void checkParentXAttrTags(AlluxioURI uri, XAttrPropagationStrategy propStrat)
      throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(uri, GET_STATUS_CONTEXT);
    Map<String, byte[]> xAttrs = fileInfo.getXAttr();
    assertNotEquals(xAttrs, null);

    switch (propStrat) {
      case NEW_PATHS:
        // Verify that the parent directories have matching xAttr
        for (int i = 1; uri.getLeadingPath(i + 1) != null; i++) {
          for (Map.Entry<String, byte[]> entry : xAttrs.entrySet()) {
            assertArrayEquals(mFileSystemMaster.getFileInfo(new AlluxioURI(uri.getLeadingPath(i)),
                GET_STATUS_CONTEXT).getXAttr().get(entry.getKey()), xAttrs.get(entry.getKey()));
          }
        }
        break;
      case LEAF_NODE:
        // Verify that the parent directories have no xAttr
        for (int i = 1; uri.getLeadingPath(i + 1) != null; i++) {
          assertNullOrEmpty(mFileSystemMaster.getFileInfo(new AlluxioURI(uri.getLeadingPath(i)),
              GET_STATUS_CONTEXT).getXAttr());
        }
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Invalid XAttrPropagationStrategy: %s", propStrat));
    }
  }

  @Test
  public void writeToReadOnlyFileWhileCreating() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    Set<String> newEntries = Sets.newHashSet("user::r--", "group::r--", "other::r--");
    // The owner of the root path will be treated as a privileged user,
    // so we need another user to do validation.
    String user = "test_user1";
    CreateFileContext context = CreateFileContext
        .mergeFrom(mNestedFileContext.getOptions())
        .setOwner(user)
        .setAcl(newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()));
    try (Closeable r = new AuthenticatedUserRule(user, Configuration.global())
        .toResource()) {
      createFileWithSingleBlock(NESTED_FILE_URI, context);
      try {
        mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
        Assert.fail("getNewBlockIdForFile after completed should fail!");
      } catch (AccessControlException e) {
        // ignored
      }
    }
  }

  @Test
  public void RecursiveDeleteForceFlushJournals() throws Exception {
    FileSystemMaster fileSystemMasterWithSpy = spy(mFileSystemMaster);
    AtomicInteger flushCount = new AtomicInteger();
    AtomicInteger closeCount = new AtomicInteger();
    when(fileSystemMasterWithSpy.createJournalContext()).thenReturn(
        new JournalContext() {
          private int mNumLogs = 0;

          @Override
          public void append(Journal.JournalEntry entry) {
            mNumLogs++;
          }

          @Override
          public void flush() throws UnavailableException {
            if (mNumLogs != 0) {
              flushCount.incrementAndGet();
              mNumLogs = 0;
            }
          }

          @Override
          public void close() throws UnavailableException {
            closeCount.incrementAndGet();
          }
        }
    );

    int level = 2;
    int numInodes = 4 * IntMath.pow(DIR_WIDTH, level) - 3;
    AlluxioURI ufsMount = createPersistedDirectories(level);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(level);
    // delete top-level directory
    fileSystemMasterWithSpy.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)
            .setAlluxioOnly(false).setUnchecked(false)));
    checkPersistedDirectoriesDeleted(level, ufsMount, Collections.EMPTY_LIST);
    assertEquals(1, closeCount.get());
    if (Configuration.getBoolean(PropertyKey.MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS)) {
      assertEquals(numInodes, flushCount.get());
    } else {
      assertEquals(0, flushCount.get());
    }
  }

  /**
   * Tests a readOnly mount for the set attribute op.
   */
  @Test
  public void exists() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    AlluxioURI alluxioFileURI = new AlluxioURI("/hello/file");

    ExistsContext neverSyncContext = ExistsContext.create(
        ExistsPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()));

    ExistsContext alwaysSyncContext = ExistsContext.create(
        ExistsPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setCommonOptions(
        FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build()));

    Assert.assertFalse(mFileSystemMaster.exists(alluxioFileURI, alwaysSyncContext));

    createTempUfsFile("ufs/hello/file");

    Assert.assertFalse(mFileSystemMaster.exists(alluxioFileURI, neverSyncContext));
    Assert.assertTrue(mFileSystemMaster.exists(alluxioFileURI, alwaysSyncContext));
    Assert.assertTrue(mFileSystemMaster.exists(alluxioFileURI, alwaysSyncContext));
    Assert.assertTrue(mFileSystemMaster.exists(alluxioFileURI, neverSyncContext));

    mTestFolder.delete();

    Assert.assertTrue(mFileSystemMaster.exists(alluxioFileURI, neverSyncContext));
    Assert.assertFalse(mFileSystemMaster.exists(alluxioFileURI, alwaysSyncContext));
  }
}
