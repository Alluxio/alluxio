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
import alluxio.exception.FileAlreadyExistsException;
import alluxio.grpc.CreateDirectoryPOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Test cases for CreateFile.
 */
public class CreateDirTest extends AlluxioTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(CreateDirTest.class);

  CreateDirTest(String dir, FileSystemContext fsContext) {
    super(dir, fsContext);

    // Adding sub test cases here...
    addSubTestCase("CreateDir",                        this::testCreateDir);
//    addSubTestCase("CreateDirOverwriteNotAllowExists", this::CreateDirOverwriteNotAllowExists);
    addSubTestCase("CreateDirOverwriteAllowExists",    this::CreateDirOverwriteAllowExists);
  }

  private Boolean testCreateDir(FileSystem fs) {
    AlluxioURI path = new AlluxioURI(getDir() + "/" + UUID.randomUUID());
    CreateDirectoryPOptions createDirectoryPOptions = CreateDirectoryPOptions.newBuilder()
        .setRecursive(false)
        .setAllowExists(false)
        .build();

    try {
      // Create a dir
      fs.createDirectory(path, createDirectoryPOptions);
    } catch (Exception e) {
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  private Boolean CreateDirOverwriteNotAllowExists(FileSystem fs) {
    AlluxioURI path = new AlluxioURI(getDir() + "/" + UUID.randomUUID());
    CreateDirectoryPOptions createDirectoryPOptions = CreateDirectoryPOptions.newBuilder()
        .setRecursive(false)
        .setAllowExists(false)
        .build();

    try {
      // Create a dir
      fs.createDirectory(path, createDirectoryPOptions);

      //Create this dir again. It should report exception
      fs.createDirectory(path, createDirectoryPOptions);
    } catch (FileAlreadyExistsException e) {
      return Boolean.TRUE;
    } catch (Exception e) {
      LOG.error("Create dir without `AllowExists` should fail with FileAlreadyExistsException");
      return Boolean.FALSE;
    }
    LOG.error("Create dir without `AllowExists` should fail with FileAlreadyExistsException");
    return Boolean.FALSE;
  }

  private Boolean CreateDirOverwriteAllowExists(FileSystem fs) {
    AlluxioURI path = new AlluxioURI(getDir() + "/" + UUID.randomUUID());
    CreateDirectoryPOptions createDirectoryPOptions = CreateDirectoryPOptions.newBuilder()
        .setRecursive(false)
        .setAllowExists(true)
        .build();

    try {
      // Create a dir
      fs.createDirectory(path, createDirectoryPOptions);

      //Create this dir again. It should succeed, because AllowExists is true.
      fs.createDirectory(path, createDirectoryPOptions);
    } catch (Exception e) {
      LOG.error("Create an existing dir with `AllowExists` should succeed. But it did not");
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  @Override
  public String toString() {
    return "CreateDirTest";
  }
}
