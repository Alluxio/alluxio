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

package alluxio.master.backcompat.ops;

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.backcompat.FsTestOp;
import alluxio.util.CommonUtils;

import java.io.IOException;

/**
 * Test for async persist functionality.
 */
public final class AsyncPersist extends FsTestOp {
  private static final AlluxioURI FILE = new AlluxioURI("/asyncPersist");
  private static final AlluxioURI NESTED_FILE = new AlluxioURI("/asyncPersistDir/nested");

  @Override
  public void apply(FileSystem fs) throws Exception {
    try (FileOutStream out = fs.createFile(FILE, CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB).setWriteType(WritePType.ASYNC_THROUGH).build())) {
      out.write("test".getBytes());
    }
    // Nested file
    try (FileOutStream out =
        fs.createFile(NESTED_FILE, CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB)
            .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build())) {
      out.write("test".getBytes());
    }
    CommonUtils.waitFor("files to be persisted", () -> {
      try {
        return fs.getStatus(FILE).isPersisted() && fs.getStatus(NESTED_FILE).isPersisted();
      } catch (IOException | AlluxioException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    assertTrue(fs.getStatus(FILE).isPersisted());
    assertTrue(fs.getStatus(NESTED_FILE).isPersisted());
    assertTrue(fs.getStatus(NESTED_FILE.getParent()).isPersisted());
  }
}
