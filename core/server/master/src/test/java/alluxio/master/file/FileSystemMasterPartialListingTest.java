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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.metastore.InodeStore;
import alluxio.wire.FileInfo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class FileSystemMasterPartialListingTest extends FileSystemMasterTestBase {

  public FileSystemMasterPartialListingTest(InodeStore.Factory factory) {
    mInodeStoreFactory = factory;
  }

  private ListStatusContext genListStatusPrefix(String prefix, boolean recursive) {
    return ListStatusContext.mergeFrom(
        ListStatusPartialPOptions.newBuilder().setPrefix(prefix).setOptions(
            ListStatusPOptions.newBuilder()
                .setLoadMetadataType(LoadMetadataPType.NEVER)
                .setRecursive(recursive)));
  }

  private ListStatusContext genListStatusStartAfter(String startAfter, boolean recursive) {
    return ListStatusContext.mergeFrom(
        ListStatusPartialPOptions.newBuilder().setStartAfter(startAfter).setOptions(
            ListStatusPOptions.newBuilder()
                .setLoadMetadataType(LoadMetadataPType.NEVER)
                .setRecursive(recursive)));
  }

  private ListStatusContext genListStatusPrefixStartAfter(
      String prefix, String startAfter, boolean recursive) {
    return ListStatusContext.mergeFrom(
        ListStatusPartialPOptions.newBuilder().setPrefix(prefix)
            .setStartAfter(startAfter).setOptions(ListStatusPOptions.newBuilder()
                .setLoadMetadataType(LoadMetadataPType.NEVER)
                .setRecursive(recursive)));
  }

  private ListStatusContext genListStatusPrefixOffsetCount(
      int batchSize, int offsetCount, boolean recursive, String prefix) {
    return ListStatusContext.mergeFrom(
        ListStatusPartialPOptions.newBuilder().setPrefix(prefix).setBatchSize(batchSize)
            .setOffsetCount(offsetCount).setOptions(ListStatusPOptions.newBuilder()
                .setLoadMetadataType(LoadMetadataPType.NEVER)
                .setRecursive(recursive)));
  }

  private ListStatusContext genListStatusPartial(
      int batchSize, long offsetId, boolean recursive, String prefix, String startAfter) {
    assertTrue("only one of offsetId and startAfter can be set",
        offsetId == 0 || startAfter.isEmpty());
    ListStatusPartialPOptions.Builder builder = ListStatusPartialPOptions.newBuilder()
        .setBatchSize(batchSize)
        .setPrefix(prefix).setOptions(
            ListStatusPOptions.newBuilder()
                .setLoadMetadataType(LoadMetadataPType.NEVER)
                .setRecursive(recursive));
    if (offsetId != 0) {
      builder.setOffsetId(offsetId);
    } else {
      builder.setStartAfter(startAfter);
    }
    return ListStatusContext.mergeFrom(builder);
  }

  @Test
  public void listStatusPartialDelete() throws Exception {
    long offset = 0;
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    // List two file from NESTED_URI, getting its offset
    ListStatusContext context = genListStatusPartial(2, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(2, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(1).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(1).getFileId();

    // The next file should be NESTED_FILE_URI2
    context = genListStatusPartial(2, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());

    // Now delete NESTED_FILE_URI, so the offset is no longer valid
    mFileSystemMaster.delete(NESTED_FILE_URI,
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));

    // Should throw an inode does not exist exception
    final long throwOffset = offset;
    assertThrows(FileDoesNotExistException.class, () -> mFileSystemMaster.listStatus(
        NESTED_URI, genListStatusPartial(1, throwOffset, true, "", "")));

    // Insert a new file with the same name
    createFileWithSingleBlock(NESTED_FILE_URI);

    // An exception should still be thrown because it has a new inode id
    assertThrows(FileDoesNotExistException.class, () -> mFileSystemMaster.listStatus(
        NESTED_URI, genListStatusPartial(1, throwOffset, true, "", "")));
  }

  @Test
  public void listStatusPartialRename() throws Exception {
    long offset = 0;
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    ListStatusContext context = genListStatusPartial(2, offset, true, "", "");
    // List two file from NESTED_URI, getting its offset
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(2, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(1).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(1).getFileId();

    // The next file should still be NESTED_FILE_URI2
    context = genListStatusPartial(2, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());

    AlluxioURI renameTo = new AlluxioURI(NESTED_FILE_URI + "1");
    // Now rename NESTED_FILE_URI, but in the same directory
    mFileSystemMaster.rename(NESTED_FILE_URI, renameTo,
        RenameContext.defaults());

    // The next file should still be NESTED_FILE_URI2
    context = genListStatusPartial(2, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());

    // Now rename the renamed file into a different directory
    mFileSystemMaster.rename(renameTo, new AlluxioURI("/moveHere"),
        RenameContext.defaults());
    // Should throw an exception since we are no longer in the same path as NESTED_URI
    final long finalOffset = offset;
    assertThrows(FileDoesNotExistException.class,
        () -> mFileSystemMaster.listStatus(NESTED_URI, genListStatusPartial(
            1, finalOffset, true, "", "")));
  }

  @Test
  public void listStatusStartAfter() throws Exception {
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);

    // list without recursion, and start after "/file",
    // the results should be sorted by name
    ListStatusContext context = genListStatusStartAfter(
        ROOT_FILE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(3, context.getTotalListings());
    assertFalse(context.isTruncated());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());

    // list with recursion, and start after "/file",
    // the results should be sorted by name
    context = genListStatusStartAfter(
        ROOT_FILE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(6, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(2).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(3).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(4).getPath());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(5).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list "/nested/test" with recursion, and start after "/file",
    // the results should be sorted by name
    context = genListStatusStartAfter("/file", true);
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list "/nested/test" with recursion, and start after "/dir",
    // the results should be sorted by name
    context = genListStatusStartAfter("/di", true);
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(3, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(2).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list "/nested/test" with recursion, and start after "/dir",
    // the results should be sorted by name
    context = genListStatusStartAfter("/dir", true);
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(2, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(1).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
  }

  @Test
  public void listStatusPrefixNestedStartAfter() throws Exception {
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(TEST_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);

    // list without recursion, with prefix "/file" and startAfter "/fi"
    ListStatusContext context = genListStatusPrefixStartAfter(
        ROOT_FILE_URI.getPath(), "/fi", false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list without recursion, with prefix "/file" and startAfter "/file"
    context = genListStatusPrefixStartAfter(
        ROOT_FILE_URI.getPath(), ROOT_FILE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(0, infos.size());
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion, with prefix "/fi" and startAfter "/file"
    context = genListStatusPrefixStartAfter(
        "/fi", ROOT_FILE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(0, infos.size());
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion, with prefix "/fi" and startAfter "/file"
    context = genListStatusPrefixStartAfter(
        "/fi", ROOT_FILE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(0, infos.size());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion, with prefix "/ne" and startAfter "/file"
    context = genListStatusPrefixStartAfter(
        "/ne", ROOT_FILE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(6, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(2).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(3).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(4).getPath());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(5).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion, with prefix "/ne" and startAfter "/nested/test"
    context = genListStatusPrefixStartAfter(
        "/ne", "/nested/test", true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(4, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(2).getPath());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(3).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
  }

  @Test
  public void listStatusPartialPrefixNestedStartAfter() throws Exception {
    List<FileInfo> infos;
    long offset;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(TEST_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);

    // list without recursion with prefix "/file" and start after "/fi",
    // the results should be sorted by name
    ListStatusContext context = genListStatusPartial(
        1, 0, false, ROOT_FILE_URI.getPath(), "/fi");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());
    assertEquals(0, infos.size());

    // list with recursion from "/nested/test/" with prefix "/file" and start after "/fi",
    // the results should be sorted by name
    context = genListStatusPartial(
        1, 0, true, ROOT_FILE_URI.getPath(), "/fi");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(0, infos.size());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion from "/" with prefix "/nest" and start after "/nested/d",
    // the results should be sorted by name
    context = genListStatusPartial(
        1, 0, true, NESTED_BASE_URI.getPath(), "/nested/test/d");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());
  }

  @Test
  public void listStatusPrefixNested() throws Exception {
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    // list one at a time without recursion, and prefix "/file",
    // the results should be sorted by name
    ListStatusContext context = genListStatusPrefix(
        ROOT_FILE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());

    // list without recursion, with a prefix that is longer than the result
    context = genListStatusPrefix(
        NESTED_FILE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(0, infos.size());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // list one at a time without recursion, and prefix "/nested",
    // the results should be sorted by name
    context = genListStatusPrefix(
        NESTED_BASE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());

    // start listing without recursion from "/nested", with a prefix of "/test/file"
    context = genListStatusPrefix("/test/file", false);
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(0, infos.size());
    assertFalse(context.isTruncated());
    assertEquals(1, context.getTotalListings());

    // list one at a time with recursion, and prefix "/nested",
    // the results should be sorted by name
    context = genListStatusPrefix(
        NESTED_BASE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(5, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(2).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(3).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(4).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // start listing with recursion from "/nested", with a prefix of "/test/file"
    context = genListStatusPrefix("/test/file", true);
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(2, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(1).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
  }

  @Test
  public void listStatusPartialPrefixNested() throws Exception {
    long offset;
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    // list one at a time without recursion, and prefix "/file",
    // the results should be sorted by name
    ListStatusContext context = genListStatusPartial(
        1, 0, false, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());
    assertEquals(0, infos.size());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 1, false, ROOT_FILE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time without recursion, and prefix "/nested",
    // the results should be sorted by name
    context = genListStatusPartial(
        1, 0, false, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertEquals(3, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());
    assertEquals(0, infos.size());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 1, false, NESTED_BASE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time without recursion from "/nested", and prefix "/test",
    // the results should be sorted by name
    context = genListStatusPartial(
        1, 0, false, "/test", "");
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_URI.toString(), infos.get(0).getPath());
    assertEquals(2, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, "/test", "");
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 1, false, "/test");
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, "/test", "");
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    assertEquals(0, infos.size());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 2, false, "/test");
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time with recursion with prefix "/file",
    // the results should be sorted by name,
    // and returned in a depth first manner
    context = genListStatusPartial(
        1, 0, true, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 1, true, ROOT_FILE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    context = genListStatusPartial(
        1, offset, true, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 2, true, ROOT_FILE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time with recursion with prefix "/nested",
    // the results should be sorted by name,
    context = genListStatusPartial(
        1, 0, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 1, true, NESTED_BASE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    // give an invalid nested prefix during the partial listing
    long finalOffset = offset;
    assertThrows(InvalidPathException.class, () ->
        mFileSystemMaster.listStatus(ROOT_URI, genListStatusPartial(
            1, finalOffset, true, ROOT_FILE_URI.getPath(), "")));

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 2, true, NESTED_BASE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 3, true, NESTED_BASE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 4, true, NESTED_BASE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 5, true, NESTED_BASE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 6, true, NESTED_BASE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time with recursion with prefix "/nested/test/file",
    // the results should be sorted by name,
    // and returned in a depth first manner
    context = genListStatusPartial(
        1, 0, true, NESTED_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 1, true, NESTED_FILE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    // use an offset count instead of offset id, the results should be the same
    context = genListStatusPrefixOffsetCount(
        1, 2, true, NESTED_FILE_URI.getPath());
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());
  }

  @Test
  public void listStatusPartialNested() throws Exception {
    long offset;
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    // list one at a time without recursion, the results should be sorted by name
    ListStatusContext context = genListStatusPartial(
        1, 0, false, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    offset = infos.get(0).getFileId();

    // use an offset count of 1 instead of offset id, should be the same as previous
    context = genListStatusPrefixOffsetCount(1, 1, false, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());

    context = genListStatusPartial(
        1, offset, false, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    assertEquals(0, infos.size());

    // use an offset count of 2 instead of offset id, should be the same as previous
    context = genListStatusPrefixOffsetCount(
        1, 2, false, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time with recursion, the results should be sorted by name
    context = genListStatusPartial(
        1, 0, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count of 1 instead of offset id, the result should be the same
    context = genListStatusPrefixOffsetCount(
        1, 1, true, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count of 2 instead of offset id, the result should be the same
    context = genListStatusPrefixOffsetCount(
        1, 2, true, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count of 3 instead of offset id, the result should be the same
    context = genListStatusPrefixOffsetCount(
        1, 3, true, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count of 4 instead of offset id, the result should be the same
    context = genListStatusPrefixOffsetCount(
        1, 4, true, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count of 5 instead of offset id, the result should be the same
    context = genListStatusPrefixOffsetCount(
        1, 5, true, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // use an offset count of 6 instead of offset id, the result should be the same
    context = genListStatusPrefixOffsetCount(
        1, 6, true, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    // use an offset count of 7 instead of offset id, the result should be the same
    context = genListStatusPrefixOffsetCount(
        1, 7, true, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());
  }

  @Test
  public void listStatusPartial() throws Exception {
    final int files = 13;
    final int batchSize = 5;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }

    long offset = 0;
    for (int i = 0; i < files; i += batchSize) {
      ListStatusContext context = genListStatusPartial(batchSize, offset,
          false, "", "");
      infos = mFileSystemMaster.listStatus(ROOT_URI, context);
      assertEquals(files, context.getTotalListings());
      if (i + infos.size() == files) {
        assertFalse(context.isTruncated());
      } else {
        assertTrue(context.isTruncated());
      }
      // Copy out filenames to use List contains.
      filenames = infos.stream().map(FileInfo::getPath).collect(
          Collectors.toCollection(ArrayList::new));
      // Compare all filenames.
      for (int j = i; j < Math.min(i + batchSize, files); j++) {
        assertTrue(
            filenames.contains(ROOT_URI.join("file" + String.format("%05d", j))
                .toString()));
      }
      assertEquals(Math.min(i + batchSize, files) - i, filenames.size());
      // start from the offset
      offset = infos.get(infos.size() - 1).getFileId();
    }

    // test a non-existing file
    assertThrows(FileDoesNotExistException.class, () -> mFileSystemMaster.listStatus(
        new AlluxioURI("/doesNotExist"), genListStatusPartial(
            batchSize, 0, false, "", "")));
  }

  @Test
  public void listStatusPartialOffsetCount() throws Exception {
    final int files = 13;
    final int batchSize = 5;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }

    int offsetCount = 0;
    for (int i = 0; i < files; i += batchSize) {
      ListStatusContext context = genListStatusPrefixOffsetCount(batchSize, offsetCount,
          false, "");
      infos = mFileSystemMaster.listStatus(ROOT_URI, context);
      assertEquals(files, context.getTotalListings());
      if (i + infos.size() == files) {
        assertFalse(context.isTruncated());
      } else {
        assertTrue(context.isTruncated());
      }
      // Copy out filenames to use List contains.
      filenames = infos.stream().map(FileInfo::getPath).collect(
          Collectors.toCollection(ArrayList::new));
      // Compare all filenames.
      for (int j = i; j < Math.min(i + batchSize, files); j++) {
        assertTrue(
            filenames.contains(ROOT_URI.join("file" + String.format("%05d", j))
                .toString()));
      }
      assertEquals(Math.min(i + batchSize, files) - i, filenames.size());
      // increase the offset by the returned count
      offsetCount += infos.size();
    }

    // a larger offset than number of files should return an empty result
    ListStatusContext context = genListStatusPrefixOffsetCount(batchSize, offsetCount + 1,
        false, "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertTrue(infos.isEmpty());
    assertEquals(files, context.getTotalListings());
  }

  @Test
  public void listStatusPartialBatchRecursive() throws Exception {
    final int files = 13;
    final int batchSize = 5;
    final int depthSize = 5;
    List<FileInfo> infos;
    // we will start a listing from each depth
    // so for each depth there will be an arraylist of files that are at that depth or greater
    ArrayList<ArrayList<String>> filenames = new ArrayList<>(depthSize);
    for (int i = 0; i < depthSize; i++) {
      filenames.add(new ArrayList<>());
    }

    // First add number files in each depth
    ArrayList<String> parent = new ArrayList<>();
    for (int j = 0; j < depthSize; j++) {
      // The directory listing will be at the start of those with smaller depth
      for (int k = 0; k < j; k++) {
        filenames.get(k).add(parent.stream().skip(1).reduce("/", String::concat) + "nxt");
      }
      for (int i = 0; i < files; i++) {
        AlluxioURI nxt = new AlluxioURI(parent.stream().reduce(
            "/", String::concat) + "file" + String.format("%05d", i));
        // the file will be in the listing of every path that has an equal or smaller depth
        for (int k = 0; k <= j; k++) {
          filenames.get(k).add(nxt.getPath());
        }
        createFileWithSingleBlock(nxt);
      }
      parent.add("nxt/");
    }

    // Start a partial listing from each depth
    StringBuilder parentPath = new StringBuilder();
    for (int i = 0; i < depthSize; i++) {
      long offset = 0;
      ArrayList<String> myFileNames = filenames.get(i);
      // do the partial listing for each batch at this depth and be sure all files are listed
      for (int j = 0; j < myFileNames.size(); j += batchSize) {
        ListStatusContext context = genListStatusPartial(
            batchSize, offset, true, "", "");
        infos = mFileSystemMaster.listStatus(new AlluxioURI("/" + parentPath), context);
        assertEquals(Math.min(myFileNames.size() - j, batchSize), infos.size());
        assertEquals(-1, context.getTotalListings());
        if (j + infos.size() == myFileNames.size()) {
          assertFalse(context.isTruncated());
        } else {
          assertTrue(context.isTruncated());
        }
        for (int k = 0; k < infos.size(); k++) {
          assertEquals(myFileNames.get(k + j), infos.get(k).getPath());
        }
        offset = infos.get(infos.size() - 1).getFileId();
      }
      // there should be no more files to list
      ListStatusContext context = genListStatusPartial(batchSize, offset, true, "", "");
      infos = mFileSystemMaster.listStatus(new AlluxioURI("/" + parentPath), context);
      assertEquals(0, infos.size());
      assertFalse(context.isTruncated());
      parentPath.append("nxt/");
    }
  }

  @Test
  public void listStatusPartialBatch() throws Exception {
    final int files = 13;
    final int batchSize = 5;
    final int depthSize = 5;
    List<FileInfo> infos;
    List<String> filenames = new ArrayList<>();

    StringBuilder parent = new StringBuilder();
    for (int j = 0; j < depthSize; j++) {
      // Test files in root directory.
      for (int i = 0; i < files; i++) {
        AlluxioURI nxt = new AlluxioURI(parent + "/file" + String.format("%05d", i));
        createFileWithSingleBlock(nxt);
      }
      parent.append("/nxt");
    }

    // go through each file without recursion
    parent = new StringBuilder();
    for (int j = 0; j < depthSize; j++) {
      long offset = 0;
      // the number of remaining files to list for this directory
      int remain;
      long listingCount;
      if (j == depthSize - 1) {
        remain = files;
        listingCount = files;
      } else {
        // +1 for the nested directory
        remain = files + 1;
        listingCount = files + 1;
      }
      for (int i = 0; i < files; i += batchSize) {
        ListStatusContext context = genListStatusPartial(
            batchSize, offset, false, "", "");
        infos = mFileSystemMaster.listStatus(new AlluxioURI("/" + parent), context);
        assertEquals(listingCount, context.getTotalListings());
        if (remain > batchSize) {
          assertTrue(context.isTruncated());
        } else {
          assertFalse(context.isTruncated());
        }
        // Copy out filenames to use List contains.
        filenames = infos.stream().map(FileInfo::getPath).collect(
            Collectors.toCollection(ArrayList::new));
        // check all the remaining files are listed
        assertEquals(Math.min(batchSize, remain), filenames.size());
        for (int k = i; k < Math.min(files, i + batchSize); k++) {
          assertTrue(filenames.contains("/" + parent + "file" + String.format("%05d", k)));
        }
        offset = infos.get(infos.size() - 1).getFileId();
        remain -= batchSize;
      }
      // listing of the nested directory
      if (j != depthSize - 1) {
        assertEquals("/" + parent + "nxt", filenames.get(filenames.size() - 1));
      }
      // all files should have been listed
      ListStatusContext context = genListStatusPartial(
          batchSize, offset, false, "", "");
      infos = mFileSystemMaster.listStatus(new AlluxioURI("/" + parent), context);
      assertFalse(context.isTruncated());
      assertEquals(0, infos.size());

      parent.append("nxt/");
    }
  }
}
