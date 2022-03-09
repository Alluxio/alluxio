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

package alluxio.client.cli.job;

import alluxio.client.file.FileSystemTestUtils;
import alluxio.grpc.WritePType;
import alluxio.job.cmd.migrate.MigrateCliConfig;
//import alluxio.job.util.JobTestUtils;
//import alluxio.job.wire.Status;
//
//import com.google.common.collect.Sets;

import org.junit.Test;

/**
 * Test for async submission.
 */
public final class AsyncSubmissionTest extends JobShellTest {
  private static final int TEST_TIMEOUT = 20;

  @Test
  public void asyncSubmission() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFileSource/file0",
            WritePType.THROUGH, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFileSource/file1",
            WritePType.THROUGH, 10);
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFileSource/file2",
            WritePType.THROUGH, 10);

    long jobId = sJobMaster.submit(new MigrateCliConfig(
            "/testFileSource", "/testFileDest", "THROUGH", false, 1));

//    long jobId1 = sJobMaster.submit(new MigrateCliConfig(
//            "/testFileSource", "/testFileDest1", "THROUGH", false, 1));

    //sJobShell.run("cancel", Long.toString(jobId));

    //waitForJobToFinish(jobId);

//    JobTestUtils
//            .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED), TEST_TIMEOUT);
//    JobTestUtils
//            .waitForJobStatus(sJobMaster, jobId1, Sets.newHashSet(Status.CANCELED), TEST_TIMEOUT);

    //sJobShell.run("stat", "-v", Long.toString(jobId));
  }
}
