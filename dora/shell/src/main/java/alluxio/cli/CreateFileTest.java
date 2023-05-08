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
import alluxio.grpc.CreateFilePOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Test cases for CreateFile.
 */
public class CreateFileTest extends AlluxioTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(CreateFileTest.class);

  CreateFileTest(String dir, FileSystemContext fsContext) {
    super(dir, fsContext);

    // Adding sub test cases here...
    addSubTestCase("CreateEmptyFile",     this::testCreateEmptyFile);
    //addSubTestCase("CreateFileOverwrite", this::testCreateFileOverwrite);
  }

  private Boolean testCreateEmptyFile(FileSystem fs) {
    AlluxioURI path = new AlluxioURI(getDir() + "/" + UUID.randomUUID());
    CreateFilePOptions createFilePOptions = CreateFilePOptions.newBuilder().build();

    try {
      // Create empty file
      fs.createFile(path, createFilePOptions).close();
    } catch (Exception e) {
      return Boolean.FALSE;
    }
    return Boolean.TRUE;
  }

  private Boolean testCreateFileOverwrite(FileSystem fs) {
    AlluxioURI path = new AlluxioURI(getDir() + "/" + UUID.randomUUID());
    CreateFilePOptions createFilePOptions = CreateFilePOptions.newBuilder().build();

    try {
      // Create empty file
      fs.createFile(path, createFilePOptions).close();

      // Create the same empty file again. This should report exception.
      fs.createFile(path, createFilePOptions).close();
    } catch (FileAlreadyExistsException e) {
      return Boolean.TRUE;
    } catch (Exception e) {
      LOG.error("Expected FileAlreadyExistsException but not got one.");
      return Boolean.FALSE;
    }
    LOG.error("Expected FileAlreadyExistsException but not got one.");
    return Boolean.FALSE;
  }

  @Override
  public String toString() {
    return "CreateFileTest";
  }
}
