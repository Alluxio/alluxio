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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedClientUserResource;
import alluxio.AuthenticatedUserRule;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.SetAclContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.metastore.InodeStore;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.util.IdUtils;
import alluxio.util.io.FileUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class FileSystemMasterFsOptsTest extends FileSystemMasterTestBase {

  public FileSystemMasterFsOptsTest(InodeStore.Factory factory) {
    mInodeStoreFactory = factory;
  }

  @Test
  public void createFileMustCacheThenCacheThrough() throws Exception {
    File file = mTestFolder.newFile();
    AlluxioURI path = new AlluxioURI("/test");
    mFileSystemMaster.createFile(path,
        CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE));

    mThrown.expect(FileAlreadyExistsException.class);
    mFileSystemMaster.createFile(path,
        CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE));
  }

  @Test
  public void createFileUsesOperationTime() throws Exception {
    AlluxioURI path = new AlluxioURI("/test");
    mFileSystemMaster.createFile(path, CreateFileContext.defaults().setOperationTimeMs(100));
    FileInfo info = mFileSystemMaster.getFileInfo(path, GetStatusContext.defaults());
    assertEquals(100, info.getLastModificationTimeMs());
    assertEquals(100, info.getLastAccessTimeMs());
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method.
   */
  @Test
  public void deleteFile() throws Exception {
    // cannot delete root
    try {
      mFileSystemMaster.delete(ROOT_URI,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)
              .setDeleteMountPoint(true)));
      fail("Should not have been able to delete the root");
    } catch (InvalidPathException e) {
      assertEquals(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage(), e.getMessage());
    }

    // delete the file
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.delete(NESTED_FILE_URI, DeleteContext.defaults());

    try {
      mBlockMaster.getBlockInfo(blockId);
      fail("Expected blockInfo to fail");
    } catch (BlockInfoException e) {
      // expected
    }

    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat1 = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat1);
    assertFalse(mBlockMaster.isBlockLost(blockId));

    // verify the file is deleted
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));

    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());
    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    mFileSystemMaster.listStatus(uri, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
    mFileSystemMaster.delete(new AlluxioURI("/mnt/local/dir1/file1"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setAlluxioOnly(true)));

    // ufs file still exists
    assertTrue(Files.exists(Paths.get(ufsMount.join("dir1").join("file1").getPath())));
    // verify the file is deleted
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/dir1/file1"), GetStatusContext
        .mergeFrom(GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method with a
   * non-empty directory.
   */
  @Test
  public void deleteNonemptyDirectory() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    String dirName = mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getName();
    try {
      mFileSystemMaster.delete(NESTED_URI, DeleteContext.defaults());
      fail("Deleting a non-empty directory without setting recursive should fail");
    } catch (DirectoryNotEmptyException e) {
      String expectedMessage =
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage(dirName);
      assertEquals(expectedMessage, e.getMessage());
    }

    // Now delete with recursive set to true.
    mFileSystemMaster.delete(NESTED_URI,
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory.
   */
  @Test
  public void deleteDir() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    // delete the dir
    mFileSystemMaster.delete(NESTED_URI,
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));

    // verify the dir is deleted
    assertEquals(-1, mFileSystemMaster.getFileId(NESTED_URI));

    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());
    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());
    // load the dir1 to alluxio
    mFileSystemMaster.listStatus(new AlluxioURI("/mnt/local"), ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
    mFileSystemMaster.delete(new AlluxioURI("/mnt/local/dir1"), DeleteContext
        .mergeFrom(DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(true)));
    // ufs directory still exists
    assertTrue(Files.exists(Paths.get(ufsMount.join("dir1").getPath())));
    // verify the directory is deleted
    Files.delete(Paths.get(ufsMount.join("dir1").getPath()));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/mnt/local/dir1")));
  }

  @Test
  public void deleteRecursiveClearsInnerInodesAndEdges() throws Exception {
    createFileWithSingleBlock(new AlluxioURI("/a/b/c/d/e"));
    createFileWithSingleBlock(new AlluxioURI("/a/b/x/y/z"));
    mFileSystemMaster.delete(new AlluxioURI("/a/b"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    assertEquals(1, mInodeStore.allEdges().size());
    assertEquals(2, mInodeStore.allInodes().size());
  }

  @Test
  public void deleteDirRecursiveWithPermissions() throws Exception {
    // userA has permissions to delete directory and nested file
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(NESTED_URI,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    }
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
  }

  @Test
  public void deleteDirRecursiveWithReadOnlyCheck() throws Exception {
    AlluxioURI rootPath = new AlluxioURI("/mnt/");
    mFileSystemMaster.createDirectory(rootPath, CreateDirectoryContext.defaults());
    // Create ufs file.
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));
    mThrown.expect(AccessControlException.class);
    // Will throw AccessControlException because /mnt/local is a readonly mount point
    mFileSystemMaster.delete(rootPath,
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
  }

  @Test
  public void deleteDirRecursiveWithInsufficientPermissions() throws Exception {
    // userA has permissions to delete directory but not one of the nested files
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE2_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(NESTED_URI,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/test/file (Permission denied"));
      assertTrue(e.getMessage().contains("/nested/test (Directory not empty)"));
    }
    // Then the nested file and the dir will be left
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    // File with permission will be deleted
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE2_URI));
  }

  @Test
  public void deleteDirRecursiveNoPermOnFile() throws Exception {
    // The structure looks like below
    // /nested
    // /nested/test/file
    // /nested/test/file2
    // /nested/test2/file
    // /nested/test2/file2
    // /nested/test2/dir
    // /nested/test2/dir/file
    // userA has no permission on /nested/test/file
    // So deleting the root will fail on:
    // /nested/, /nested/test/, /nested/test/file
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file2"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/dir/file"));

    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE2_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/dir/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(new AlluxioURI("/nested"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/test/file (Permission denied"));
      assertTrue(e.getMessage().contains("/nested/test (Directory not empty)"));
    }
    // The existing files/dirs will be: /, /nested/, /nested/test/, /nested/test/file
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    // The other files should be deleted successfully
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE2_URI));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir/file")));
  }

  @Test
  public void deleteDirRecursiveNoPermOnFileDiffOrder() throws Exception {
    // The structure looks like below
    // /nested
    // /nested/test/file
    // /nested/test/file2
    // /nested/test2/file
    // /nested/test2/file2
    // /nested/test2/dir
    // /nested/test2/dir/file
    // userA has no permission on /nested/test/file2
    // So deleting the root will fail on:
    // /nested/, /nested/test/, /nested/test/file2
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file2"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/dir/file"));

    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE2_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/dir/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(new AlluxioURI("/nested"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/test/file2 (Permission denied"));
      assertTrue(e.getMessage().contains("/nested/test (Directory not empty)"));
    }
    // The existing files/dirs will be: /, /nested/, /nested/test/, /nested/test/file
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE2_URI));
    // The other files should be deleted successfully
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir/file")));
  }

  @Test
  public void deleteNestedDirRecursiveNoPermOnFile() throws Exception {
    // The structure looks like below
    // /nested
    // /nested/nested
    // /nested/nested/test/file
    // /nested/nested/test/file2
    // userA has no permission on /nested/nested/test/file
    // So deleting the root will fail on:
    // /nested/, /nested/nested, /nested/nested/test/, /nested/nested/test/file
    createFileWithSingleBlock(new AlluxioURI("/nested/nested/test/file"));
    createFileWithSingleBlock(new AlluxioURI("/nested/nested/test/file2"));

    mFileSystemMaster.setAttribute(new AlluxioURI("/nested"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/nested"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/nested/test"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/nested/test/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/nested/test/file2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(new AlluxioURI("/nested"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/nested/test/file (Permission denied"));
      assertTrue(e.getMessage().contains("/nested/nested/test (Directory not empty)"));
      assertTrue(e.getMessage().contains("/nested/nested (Directory not empty)"));
      assertTrue(e.getMessage().contains("/nested (Directory not empty)"));
    }
    // The existing files/dirs will be: /, /nested/, /nested/nested/,
    // /nested/nested/test/, /nested/test/file
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/nested/test")));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/nested/test/file")));
    // The other files should be deleted successfully
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/nested/test/file2")));
  }

  @Test
  public void deleteDirRecursiveNoPermOnDir() throws Exception {
    // The structure looks like below
    // /nested/
    //
    // /nested/test/
    // /nested/test/file
    // /nested/test/file2
    // /nested/test/dir
    // /nested/test/dir/file
    //
    // /nested/test2/
    // /nested/test2/file
    // /nested/test2/file2
    // /nested/test2/dir
    // /nested/test2/dir/file
    // userA has no permission on /nested/test/file
    // So deleting the root will fail on:
    // /nested/, /nested/test/, /nested/test/file
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_FILE_URI);
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file2"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/dir/file"));

    // No permission on the dir, therefore all the files will be kept
    // although the user has permission over those nested files
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE2_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_DIR_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test/dir/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    // The user has permission over everything under this dir, so everything will be removed
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/dir/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(new AlluxioURI("/nested"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/test (Permission denied"));
      assertTrue(e.getMessage().contains("/nested (Directory not empty)"));
    }
    // The existing files/dirs will be: /, /nested/, /nested/test/
    // and everything under /nested/test/
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test/file2")));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_DIR_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_DIR_FILE_URI));
    // The other files should be deleted successfully
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir/file")));
  }

  /**
   * Tests the {@link FileSystemMaster#rename(AlluxioURI, AlluxioURI, RenameContext)} method.
   */
  @Test
  public void rename() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);

    // try to rename a file to root
    try {
      mFileSystemMaster.rename(NESTED_FILE_URI, ROOT_URI, RenameContext.defaults());
      fail("Renaming to root should fail.");
    } catch (InvalidPathException e) {
      assertEquals(ExceptionMessage.RENAME_CANNOT_BE_TO_ROOT.getMessage(), e.getMessage());
    }

    // move root to another path
    try {
      mFileSystemMaster.rename(ROOT_URI, TEST_URI, RenameContext.defaults());
      fail("Should not be able to rename root");
    } catch (InvalidPathException e) {
      assertEquals(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage(), e.getMessage());
    }

    // move to existing path
    try {
      mFileSystemMaster.rename(NESTED_FILE_URI, NESTED_URI, RenameContext.defaults());
      fail("Should not be able to overwrite existing file.");
    } catch (FileAlreadyExistsException e) {
      assertEquals(String
          .format("Cannot rename because destination already exists. src: %s dst: %s",
              NESTED_FILE_URI.getPath(), NESTED_URI.getPath()), e.getMessage());
    }

    // move a nested file to a root file
    mFileSystemMaster.rename(NESTED_FILE_URI, TEST_URI, RenameContext.defaults());
    assertEquals(mFileSystemMaster.getFileInfo(TEST_URI, GET_STATUS_CONTEXT).getPath(),
        TEST_URI.getPath());

    // move a file where the dst is lexicographically earlier than the source
    AlluxioURI newDst = new AlluxioURI("/abc_test");
    mFileSystemMaster.rename(TEST_URI, newDst, RenameContext.defaults());
    assertEquals(mFileSystemMaster.getFileInfo(newDst, GET_STATUS_CONTEXT).getPath(),
        newDst.getPath());
  }

  /**
   * Tests the {@link FileSystemMaster#getFileInfo(AlluxioURI, GetStatusContext)} method.
   */
  @Test
  public void getFileInfo() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long fileId;
    FileInfo info;

    fileId = mFileSystemMaster.getFileId(ROOT_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(ROOT_URI.getPath(), info.getPath());
    assertEquals(ROOT_URI.getPath(),
        mFileSystemMaster.getFileInfo(ROOT_URI, GET_STATUS_CONTEXT).getPath());

    fileId = mFileSystemMaster.getFileId(NESTED_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(NESTED_URI.getPath(), info.getPath());
    assertEquals(NESTED_URI.getPath(),
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getPath());

    fileId = mFileSystemMaster.getFileId(NESTED_FILE_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(NESTED_FILE_URI.getPath(), info.getPath());
    assertEquals(NESTED_FILE_URI.getPath(),
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getPath());

    // Test non-existent id.
    try {
      mFileSystemMaster.getFileInfo(fileId + 1234);
      fail("getFileInfo() for a non-existent id should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    // Test non-existent URIs.
    try {
      mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
    try {
      mFileSystemMaster.getFileInfo(TEST_URI, GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
    try {
      mFileSystemMaster.getFileInfo(NESTED_URI.join("DNE"), GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void getFileInfoWithLoadMetadata() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createFile(Paths.get(ufsMount.join("file").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileInfo should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/file");
    assertEquals(uri.getPath(),
        mFileSystemMaster.getFileInfo(uri, GET_STATUS_CONTEXT).getPath());

    // getFileInfo should have loaded another file, so now 4 paths exist.
    assertEquals(4, countPaths());
  }

  @Test
  public void getFileIdWithLoadMetadata() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createFile(Paths.get(ufsMount.join("file").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/file");
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(uri));

    // getFileId should have loaded another file, so now 4 paths exist.
    assertEquals(4, countPaths());
  }

  @Test
  public void listStatusWithLoadMetadataNever() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    try {
      mFileSystemMaster.listStatus(uri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
      fail("Exception expected");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    assertEquals(3, countPaths());
  }

  @Test
  public void listStatusWithLoadMetadataOnce() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    List<FileInfo> fileInfoList =
        mFileSystemMaster.listStatus(uri, ListStatusContext.defaults());
    Set<String> paths = new HashSet<>();
    for (FileInfo fileInfo : fileInfoList) {
      paths.add(fileInfo.getPath());
    }
    assertEquals(2, paths.size());
    assertTrue(paths.contains("/mnt/local/dir1/file1"));
    assertTrue(paths.contains("/mnt/local/dir1/file2"));
    // listStatus should have loaded another 3 files (dir1, dir1/file1, dir1/file2), so now 6
    // paths exist.
    assertEquals(6, countPaths());
  }

  @Test
  public void listStatusWithLoadMetadataAlways() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    List<FileInfo> fileInfoList =
        mFileSystemMaster.listStatus(uri, ListStatusContext.defaults());
    assertEquals(0, fileInfoList.size());
    // listStatus should have loaded another files (dir1), so now 4 paths exist.
    assertEquals(4, countPaths());

    // Add two files.
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));

    fileInfoList =
        mFileSystemMaster.listStatus(uri, ListStatusContext.defaults());
    assertEquals(0, fileInfoList.size());
    // No file is loaded since dir1 has been loaded once.
    assertEquals(4, countPaths());

    fileInfoList = mFileSystemMaster.listStatus(uri, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
    Set<String> paths = new HashSet<>();
    for (FileInfo fileInfo : fileInfoList) {
      paths.add(fileInfo.getPath());
    }
    assertEquals(2, paths.size());
    assertTrue(paths.contains("/mnt/local/dir1/file1"));
    assertTrue(paths.contains("/mnt/local/dir1/file2"));
    // listStatus should have loaded another 2 files (dir1/file1, dir1/file2), so now 6
    // paths exist.
    assertEquals(6, countPaths());
  }

  /**
   * Tests listing status on a non-persisted directory.
   */
  @Test
  public void listStatusWithLoadMetadataNonPersistedDir() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // Create a drectory in alluxio which is not persisted.
    AlluxioURI folder = new AlluxioURI("/mnt/local/folder");
    mFileSystemMaster.createDirectory(folder, CreateDirectoryContext.defaults());

    assertFalse(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder"), GET_STATUS_CONTEXT)
            .isPersisted());

    // Create files in ufs.
    Files.createDirectory(Paths.get(ufsMount.join("folder").getPath()));
    Files.createFile(Paths.get(ufsMount.join("folder").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("folder").join("file2").getPath()));

    // getStatus won't mark folder as persisted.
    assertFalse(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder"), GET_STATUS_CONTEXT)
            .isPersisted());

    List<FileInfo> fileInfoList =
        mFileSystemMaster.listStatus(folder, ListStatusContext.defaults());
    assertEquals(2, fileInfoList.size());
    // listStatus should have loaded files (folder, folder/file1, folder/file2), so now 6 paths
    // exist.
    assertEquals(6, countPaths());

    Set<String> paths = new HashSet<>();
    for (FileInfo f : fileInfoList) {
      paths.add(f.getPath());
    }
    assertEquals(2, paths.size());
    assertTrue(paths.contains("/mnt/local/folder/file1"));
    assertTrue(paths.contains("/mnt/local/folder/file2"));

    assertTrue(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder"), GET_STATUS_CONTEXT)
            .isPersisted());
  }

  @Test
  public void listStatus() throws Exception {
    final int files = 10;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
    assertEquals(files, infos.size());
    // Copy out filenames to use List contains.
    filenames = new ArrayList<>();
    for (FileInfo info : infos) {
      filenames.add(info.getPath());
    }
    // Compare all filenames.
    for (int i = 0; i < files; i++) {
      assertTrue(
          filenames.contains(ROOT_URI.join("file" + String.format("%05d", i)).toString()));
    }

    // Test single file.
    createFileWithSingleBlock(ROOT_FILE_URI);
    infos = mFileSystemMaster.listStatus(ROOT_FILE_URI, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.getPath(), infos.get(0).getPath());

    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }
    infos = mFileSystemMaster.listStatus(NESTED_URI, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
    assertEquals(files, infos.size());
    // Copy out filenames to use List contains.
    filenames = new ArrayList<>();
    for (FileInfo info : infos) {
      filenames.add(info.getPath());
    }
    // Compare all filenames.
    for (int i = 0; i < files; i++) {
      assertTrue(
          filenames.contains(NESTED_URI.join("file" + String.format("%05d", i)).toString()));
    }

    // Test non-existent URIs.
    try {
      mFileSystemMaster.listStatus(NESTED_URI.join("DNE"), ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
      fail("listStatus() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void listStatusRecursive() throws Exception {
    final int files = 10;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }

    // Test recursive listStatus
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    // 10 files in each directory, 2 levels of directories
    assertEquals(files + files + 2, infos.size());

    filenames = new ArrayList<>();
    for (FileInfo info : infos) {
      filenames.add(info.getPath());
    }
    for (int i = 0; i < files; i++) {
      assertTrue(
          filenames.contains(ROOT_URI.join("file" + String.format("%05d", i)).toString()));
    }
    for (int i = 0; i < files; i++) {
      assertTrue(
          filenames.contains(NESTED_URI.join("file" + String.format("%05d", i)).toString()));
    }
  }

  @Test
  public void listStatusRecursivePermissions() throws Exception {
    final int files = 10;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }

    // Test with permissions
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext.mergeFrom(SetAttributePOptions
        .newBuilder().setMode(new Mode((short) 0400).toProto()).setRecursive(true)));
    try (Closeable r = new AuthenticatedUserRule("test_user1", Configuration.global())
        .toResource()) {
      // Test recursive listStatus
      infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
          .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));

      // 10 files in the root directory, 2 level of directories
      assertEquals(files + 2, infos.size());
    }
  }

  @Test
  public void listStatusRecursiveLoadMetadata() throws Exception {
    final int files = 10;
    List<FileInfo> infos;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }

    FileUtils.createFile(Paths.get(mUnderFS).resolve("ufsfile1").toString());
    // Test interaction between recursive and loadMetadata
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(false)));

    assertEquals(files + 1  , infos.size());

    FileUtils.createFile(Paths.get(mUnderFS).resolve("ufsfile2").toString());
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(false)));
    assertEquals(files + 1  , infos.size());

    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(false)));
    assertEquals(files + 2, infos.size());

    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }

    FileUtils.createFile(Paths.get(mUnderFS).resolve("nested/test/ufsnestedfile1").toString());
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(true)));
    // 2 sets of files, 2 files inserted at root, 2 directories nested and test,
    // 1 file ufsnestedfile1
    assertEquals(files + files + 2 + 2 + 1, infos.size());

    FileUtils.createFile(Paths.get(mUnderFS).resolve("nested/test/ufsnestedfile2").toString());
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(true)));
    assertEquals(files + files + 2 + 2 + 1, infos.size());

    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    assertEquals(files + files + 2 + 2 + 2, infos.size());
  }

  /**
   * Tests that an exception is thrown when trying to create a file in a non-existing directory
   * without setting the {@code recursive} flag.
   */
  @Test
  public void renameUnderNonexistingDir() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/nested/test"));
    CreateFileContext context = CreateFileContext
        .mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB));
    mFileSystemMaster.createFile(TEST_URI, context);

    // nested dir
    mFileSystemMaster.rename(TEST_URI, NESTED_FILE_URI, RenameContext.defaults());
  }

  @Test
  public void renameToNonExistentParent() throws Exception {
    CreateFileContext context = CreateFileContext.mergeFrom(
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true));
    mFileSystemMaster.createFile(NESTED_URI, context);

    try {
      mFileSystemMaster.rename(NESTED_URI, new AlluxioURI("/testDNE/b"), RenameContext.defaults());
      fail("Rename to a non-existent parent path should not succeed.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  /**
   * Tests that an exception is thrown when trying to rename a file to a prefix of the original
   * file.
   */
  @Test
  public void renameToSubpath() throws Exception {
    mFileSystemMaster.createFile(NESTED_URI, mNestedFileContext);
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Traversal failed for path /nested/test/file. "
        + "Component 2(test) is a file, not a directory");
    mFileSystemMaster.rename(NESTED_URI, NESTED_FILE_URI, RenameContext.defaults());
  }

  @Test
  public void setDefaultAclforFile() throws Exception {
    SetAclContext context = SetAclContext.defaults();
    createFileWithSingleBlock(NESTED_FILE_URI);

    Set<String> newEntries = Sets.newHashSet("default:user::rwx",
        "default:group::rwx", "default:other::r-x");

    mThrown.expect(UnsupportedOperationException.class);
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);
  }

  @Test
  public void setDefaultAcl() throws Exception {
    SetAclContext context = SetAclContext.defaults();
    createFileWithSingleBlock(NESTED_FILE_URI);
    Set<String> entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(0, entries.size());

    // replace
    Set<String> newEntries = Sets.newHashSet("default:user::rwx",
        "default:group::rwx", "default:other::r-x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(newEntries, entries);

    // replace
    newEntries = Sets.newHashSet("default:user::rw-", "default:group::r--", "default:other::r--");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);
    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(newEntries, entries);

    // modify existing
    newEntries = Sets.newHashSet("default:user::rwx", "default:group::rw-", "default:other::r-x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(newEntries, entries);

    // modify add
    Set<String> oldEntries = new HashSet<>(entries);
    newEntries = Sets.newHashSet("default:user:usera:---", "default:group:groupa:--x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertTrue(entries.containsAll(oldEntries));
    assertTrue(entries.containsAll(newEntries));
    assertTrue(entries.contains("default:mask::rwx"));

    // modify existing and add
    newEntries = Sets.newHashSet("default:user:usera:---", "default:group:groupa:--x",
        "default:other::r-x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertTrue(entries.containsAll(newEntries));

    // remove default
    mFileSystemMaster
        .setAcl(NESTED_URI, SetAclAction.REMOVE_DEFAULT, Collections.emptyList(), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(0, entries.size());

    // remove
    newEntries =
        Sets.newHashSet("default:user:usera:---", "default:user:userb:rwx",
            "default:group:groupa:--x", "default:group:groupb:-wx");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);
    oldEntries = new HashSet<>(entries);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertTrue(entries.containsAll(oldEntries));

    Set<String> deleteEntries = Sets.newHashSet("default:user:userb:rwx",
        "default:group:groupa:--x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.REMOVE,
        deleteEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    Set<String> remainingEntries = new HashSet<>(newEntries);
    assertTrue(remainingEntries.removeAll(deleteEntries));
    assertTrue(entries.containsAll(remainingEntries));

    final Set<String> finalEntries = entries;
    assertTrue(deleteEntries.stream().noneMatch(finalEntries::contains));
  }

  @Test
  public void setAcl() throws Exception {
    SetAclContext context = SetAclContext.defaults();
    createFileWithSingleBlock(NESTED_FILE_URI);

    Set<String> entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(3, entries.size());

    // replace
    Set<String> newEntries = Sets.newHashSet("user::rwx", "group::rwx", "other::rwx");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(newEntries, entries);

    // replace
    newEntries = Sets.newHashSet("user::rw-", "group::r--", "other::r--");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(newEntries, entries);

    // modify existing
    newEntries = Sets.newHashSet("user::rwx", "group::r--", "other::r-x");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(newEntries, entries);

    // modify add
    Set<String> oldEntries = new HashSet<>(entries);
    newEntries = Sets.newHashSet("user:usera:---", "group:groupa:--x");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertTrue(entries.containsAll(oldEntries));
    assertTrue(entries.containsAll(newEntries));
    // check if the mask got updated correctly
    assertTrue(entries.contains("mask::r-x"));

    // modify existing and add
    newEntries = Sets.newHashSet("user:usera:---", "group:groupa:--x", "other::r-x");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertTrue(entries.containsAll(newEntries));

    // remove all
    mFileSystemMaster
        .setAcl(NESTED_FILE_URI, SetAclAction.REMOVE_ALL, Collections.emptyList(), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(3, entries.size());

    // remove
    newEntries =
        Sets.newHashSet("user:usera:---", "user:userb:rwx", "group:groupa:--x", "group:groupb:-wx");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);
    oldEntries = new HashSet<>(entries);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertTrue(entries.containsAll(oldEntries));

    Set<String> deleteEntries = Sets.newHashSet("user:userb:rwx", "group:groupa:--x");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.REMOVE,
        deleteEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    Set<String> remainingEntries = new HashSet<>(newEntries);
    assertTrue(remainingEntries.removeAll(deleteEntries));
    assertTrue(entries.containsAll(remainingEntries));

    final Set<String> finalEntries = entries;
    assertTrue(deleteEntries.stream().noneMatch(finalEntries::contains));
  }

  @Test
  public void setRecursiveAcl() throws Exception {
    final int files = 10;
    SetAclContext context =
        SetAclContext.mergeFrom(SetAclPOptions.newBuilder().setRecursive(true));

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }

    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_DIR_URI.join("file" + String.format("%05d", i)));
    }

    // replace
    Set<String> newEntries = Sets.newHashSet("user::rw-", "group::r-x", "other::-wx");
    mFileSystemMaster.setAcl(ROOT_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    List<FileInfo> infos =
        mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
            .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(true)));
    assertEquals(files * 3 + 3, infos.size());
    for (FileInfo info : infos) {
      assertEquals(newEntries, Sets.newHashSet(info.convertAclToStringEntries()));
    }
  }
}
