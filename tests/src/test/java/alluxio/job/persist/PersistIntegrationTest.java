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
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.JobIntegrationTest;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.LocalAlluxioClusterResource;
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
        CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE));
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

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {
          PropertyKey.Name.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS, "1ms",
          PropertyKey.Name.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "50ms",
          PropertyKey.Name.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "50ms"})
  public void persistTimeoutTest() throws Exception {
    // write a file in alluxio only
    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE));
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();
    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    // kill job worker
    mLocalAlluxioJobCluster.getWorker().stop();
    // persist the file
    FileSystemContext context = FileSystemContext.get();
    FileSystemMasterClient client = context.acquireMasterClient();
    try {
      client.scheduleAsyncPersist(new AlluxioURI(TEST_URI));
    } finally {
      context.releaseMasterClient(client);
    }
    // verify scheduled to be persisted
    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());
    // wait 100ms
    Thread.sleep(100);
    // verify timeout and reverted to not persisted
    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    // restart master
    mLocalAlluxioClusterResource.get().restartMasters();
    // verify not persisted
    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
    String ufsPath = status.getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.Factory.create(ufsPath);
    Assert.assertFalse(ufs.exists(ufsPath));
  }
}
