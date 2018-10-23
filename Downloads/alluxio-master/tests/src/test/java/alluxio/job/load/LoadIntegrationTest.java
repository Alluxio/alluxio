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

package alluxio.job.load;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.JobIntegrationTest;
import alluxio.master.file.meta.PersistenceState;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for {@link LoadDefinition}.
 */
public final class LoadIntegrationTest extends JobIntegrationTest {
  private static final String TEST_URI = "/test";

  /**
   * Tests that running the load job will load a file into memory, and that running the job again
   * will not create any tasks.
   */
  @Test
  public void loadTest() throws Exception {
    // write a file outside of Alluxio
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.THROUGH));
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // check the file is completed but not in Alluxio
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(0, status.getInMemoryPercentage());

    // run the load job
    waitForJobToFinish(mJobMaster.run(new LoadConfig("/test", null)));

    // check the file is fully in memory
    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(100, status.getInMemoryPercentage());

    // a second load should work too, no worker is selected
    long jobId = mJobMaster.run(new LoadConfig("/test", null));
    Assert.assertTrue(mJobMaster.getStatus(jobId).getTaskInfoList().isEmpty());
  }

  @Test
  public void loadManyBlocks() throws Exception {
    // write a file outside of Alluxio
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath, CreateFileOptions.defaults()
        .setBlockSizeBytes(16 * Constants.KB).setWriteType(WriteType.THROUGH));
    byte[] bytes = BufferUtils.getIncreasingByteArray(500 * Constants.KB);
    os.write(bytes);
    os.close();

    // check the file is completed but not in Alluxio
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    Assert.assertEquals(0, status.getInMemoryPercentage());

    // run the load job
    waitForJobToFinish(mJobMaster.run(new LoadConfig("/test", null)));

    // check the file is fully in memory
    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(100, status.getInMemoryPercentage());

    // a second load should work too, no worker is selected
    long jobId = mJobMaster.run(new LoadConfig("/test", null));
    Assert.assertTrue(mJobMaster.getStatus(jobId).getTaskInfoList().isEmpty());
  }
}
