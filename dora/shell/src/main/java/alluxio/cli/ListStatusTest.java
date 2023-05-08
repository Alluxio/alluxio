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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Test cases for CreateFile.
 */
public class ListStatusTest extends AlluxioTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(ListStatusTest.class);

  ListStatusTest(String dir, FileSystemContext fsContext) {
    super(dir, fsContext);

    // Adding sub test cases here...
    addSubTestCase("listStatusOfDir",                        this::testListStatus);
    addSubTestCase("listStatusOfFile",                       this::testListStatusOfFile);
    addSubTestCase("testListStatusUpdated",                  this::testListStatusUpdated);
  }

  private Boolean testListStatus(FileSystem fs) {
    final int NUNBER_OF_FILES = 10;
    AlluxioURI dir = new AlluxioURI(getDir() + "/" + UUID.randomUUID());
    CreateDirectoryPOptions createDirectoryPOptions = CreateDirectoryPOptions.newBuilder()
        .setRecursive(true)
        .setAllowExists(true)
        .build();
    List<String> expected = new ArrayList<>();

    try {
      // Create the parent dir
      fs.createDirectory(dir, createDirectoryPOptions);
    } catch (Exception e) {
      LOG.error("Can not create dir {}: {}", dir, e);
      return Boolean.FALSE;
    }

    CreateFilePOptions createFilePOptions = CreateFilePOptions.newBuilder().build();

    try {
      for (int i = 0; i < NUNBER_OF_FILES; i++) {
        String oneFileName = "some_plain_file_" + i;
        String fullPath = PathUtils.concatPath(dir, oneFileName);
        expected.add(fullPath);
        fs.createFile(new AlluxioURI(fullPath), createFilePOptions).close();
      }
      List<URIStatus> listOfFiles = fs.listStatus(dir);
      List<String> got = new ArrayList<>();
      for (URIStatus s: listOfFiles) {
        got.add(s.getPath());
      }
      if (got.size() != NUNBER_OF_FILES) {
        LOG.error("The number of result files {} is not expected. ", got);
        return Boolean.FALSE;
      }
      expected.removeAll(got);
      if (expected.size() != 0) {
        LOG.error("the following files are not listed: {}", expected);
        return Boolean.FALSE;
      }
    } catch (Exception e) {
      LOG.error("error in ls: {}", e);
      return Boolean.FALSE;
    }

    try {
      fs.delete(dir, DeletePOptions.newBuilder().setRecursive(true).build());
    } catch (Exception e) {
      LOG.error("error to remove parent dir: {}", e);
      return Boolean.FALSE;
    }

    return Boolean.TRUE;
  }

  private Boolean testListStatusOfFile(FileSystem fs) {
    AlluxioURI dir = new AlluxioURI(getDir() + "/" + UUID.randomUUID());
    CreateDirectoryPOptions createDirectoryPOptions = CreateDirectoryPOptions.newBuilder()
        .setRecursive(true)
        .setAllowExists(true)
        .build();
    List<String> expected = new ArrayList<>();

    try {
      // Create the parent dir
      fs.createDirectory(dir, createDirectoryPOptions);
    } catch (Exception e) {
      LOG.error("Can not create dir {}: {}", dir, e);
      return Boolean.FALSE;
    }

    CreateFilePOptions createFilePOptions = CreateFilePOptions.newBuilder().build();
    String fileName = PathUtils.concatPath(dir, "some_plain_file");
    try {
      fs.createFile(new AlluxioURI(fileName), createFilePOptions).close();
      // This is a plain file. ListStatus() for a file should work.
      List<URIStatus> listOfFiles = fs.listStatus(new AlluxioURI(fileName));
      if (listOfFiles.size() != 1) {
        return Boolean.FALSE;
      }
      if (fileName.compareTo(listOfFiles.get(0).getPath()) != 0) {
        return Boolean.FALSE;
      }
    } catch (Exception e) {
      return Boolean.FALSE;
    }
    try {
      fs.delete(dir, DeletePOptions.newBuilder().setRecursive(true).build());
    } catch (Exception e) {
      LOG.error("error to remove parent dir: {}", e);
      return Boolean.FALSE;
    }

    return Boolean.TRUE;
  }

  /**
   * Test the case that a dir is updated after last listStatus().
   *
   * @param fs the File system
   * @return true if passes, false if fails
   */
  private Boolean testListStatusUpdated(FileSystem fs) {
    final int NUNBER_OF_FILES = 10;
    AlluxioURI dir = new AlluxioURI(getDir() + "/" + UUID.randomUUID());
    CreateDirectoryPOptions createDirectoryPOptions = CreateDirectoryPOptions.newBuilder()
        .setRecursive(true)
        .setAllowExists(true)
        .build();

    try {
      // Create the parent dir
      fs.createDirectory(dir, createDirectoryPOptions);
    } catch (Exception e) {
      LOG.error("Can not create dir {}: {}", dir, e);
      return Boolean.FALSE;
    }

    CreateFilePOptions createFilePOptions = CreateFilePOptions.newBuilder().build();

    try {
      List<String> expectedAll = new ArrayList<>();

      // Step 1: Create NUNBER_OF_FILES in a dir and listStatus()
      for (int i = 0; i < NUNBER_OF_FILES; i++) {
        String oneFileName = "some_plain_file_" + i;
        String fullPath = PathUtils.concatPath(dir, oneFileName);
        expectedAll.add(fullPath);
        fs.createFile(new AlluxioURI(fullPath), createFilePOptions).close();
      }
      List<URIStatus> listOfFiles1 = fs.listStatus(dir);
      List<String> got1 = new ArrayList<>();
      for (URIStatus s: listOfFiles1) {
        got1.add(s.getPath());
      }
      if (got1.size() != NUNBER_OF_FILES) {
        LOG.error("The number of result files {} is not expected. ", got1);
        return Boolean.FALSE;
      }

      List<String> expected1 = new ArrayList<>();
      expected1.addAll(expectedAll);
      expected1.removeAll(got1);
      if (expected1.size() != 0) {
        LOG.error("the following files are not listed: {}", expected1);
        return Boolean.FALSE;
      }
      listOfFiles1 = null;
      got1 = null;
      expected1 = null;

      // Step 2: create a new file in the same dir.
      // Worker has listStatus() result cached. The cache is still valid.
      // The result will not include the newly created file.
      // `expectedAll` does not change at this moment.
      String aNewFile = "a_new_file";
      String fullPath = PathUtils.concatPath(dir, aNewFile);
      fs.createFile(new AlluxioURI(fullPath), createFilePOptions).close();

      List<URIStatus> listOfFiles2 = fs.listStatus(dir);
      List<String> got2 = new ArrayList<>();
      for (URIStatus s: listOfFiles2) {
        got2.add(s.getPath());
      }
      if (got2.size() != NUNBER_OF_FILES) {
        LOG.error("The number of result files {} is not expected. ", got2);
        return Boolean.FALSE;
      }
      List<String> expected2 = new ArrayList<>();
      expected2.addAll(expectedAll);
      expected2.removeAll(got2);
      if (expected2.size() != 0) {
        LOG.error("the following files are not listed: {}", expected2);
        return Boolean.FALSE;
      }
      listOfFiles2 = null;
      got2 = null;
      expected2 = null;

      // Step 3: Forcibly invalidate the cache of this dir.
      ListStatusPOptions options = ListStatusPOptions.newBuilder()
          .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
              .setSyncIntervalMs(0)
              .build())
          .build();

      // This time, we add this new file into expected list.
      // The new file should appear in the listStatus() result
      expectedAll.add(fullPath);

      List<URIStatus> listOfFiles3 = fs.listStatus(dir, options);
      List<String> got3 = new ArrayList<>();
      for (URIStatus s: listOfFiles3) {
        got3.add(s.getPath());
      }
      if (got3.size() != NUNBER_OF_FILES + 1) {
        LOG.error("The number of result files {} is not expected. ", got3);
        return Boolean.FALSE;
      }
      List<String> expected3 = new ArrayList<>();
      expected3.addAll(expectedAll);
      expected3.removeAll(got3);
      if (expected3.size() != 0) {
        LOG.error("the following files are not listed: {}", expected3);
        return Boolean.FALSE;
      }
    } catch (Exception e) {
      LOG.error("error in ls: {}", e);
      return Boolean.FALSE;
    }

    try {
      fs.delete(dir, DeletePOptions.newBuilder().setRecursive(true).build());
    } catch (Exception e) {
      LOG.error("error to remove parent dir: {}", e);
      return Boolean.FALSE;
    }

    return Boolean.TRUE;
  }

  @Override
  public String toString() {
    return "ListStatusTest";
  }
}
