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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.backcompat.FsTestOp;
import alluxio.master.file.meta.PersistenceState;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import java.util.Arrays;

/**
 * Test for async persist functionality.
 */
public final class AsyncPersist extends FsTestOp {
  private static final AlluxioURI FILE = new AlluxioURI("/asyncPersist");

  @Override
  public void apply(FileSystem fs) throws Exception {
    try (FileOutStream out = fs.createFile(FILE, CreateFileOptions.defaults()
        .setBlockSizeBytes(Constants.KB).setWriteType(WriteType.ASYNC_THROUGH))) {
      out.write("test".getBytes());
    }
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    URIStatus status = fs.getStatus(FILE);
    Assert.assertThat("Async persisted file should be PERSISTED or TO_BE_PERSISTED",
        Arrays.asList(PersistenceState.PERSISTED.name(), PersistenceState.TO_BE_PERSISTED.name()),
        CoreMatchers.hasItem(status.getPersistenceState()));
  }
}
