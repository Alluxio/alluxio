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

package alluxio.job.persist;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemClientOptions;
import alluxio.client.file.URIStatus;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.JobIntegrationTest;
import alluxio.master.file.meta.PersistenceState;
import alluxio.underfs.UnderFileSystem;

import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for {@link PersistDefinition}.
 */
public final class PersistIntegrationTest extends JobIntegrationTest {
  private static final String TEST_URI = "/test";

  /**
   * Tests persisting a file.
   */
  @Test
  public void persistTest() throws Exception {
    // write a file in alluxio only
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.WRITE_MUST_CACHE).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    // run the persist job and check that it succeeds
    waitForJobToFinish(mJobMaster.run(new PersistConfig("/test", 1, true, status.getUfsPath())));
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath);
    Assert.assertTrue(ufs.exists(ufsPath));

    // run the persist job again with the overwrite flag and check that it succeeds
    waitForJobToFinish(mJobMaster.run(new PersistConfig("/test", 1, true, status.getUfsPath())));
    Assert.assertTrue(ufs.exists(ufsPath));

    // run the persist job again without the overwrite flag and check it fails
    final long jobId = mJobMaster.run(new PersistConfig("/test", 1, false, status.getUfsPath()));
    waitForJobFailure(jobId);
  }
}
