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

import alluxio.AlluxioURI;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.plan.persist.PersistConfig;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;

import java.util.concurrent.TimeoutException;

public abstract class JobShellTest extends AbstractFileSystemShellTest {

  protected long runPersistJob() throws Exception {
    return runPersistJob("/test");
  }

  protected long runPersistJob(String pathStr) throws Exception {
    // write a file in alluxio only
    AlluxioURI filePath = new AlluxioURI(pathStr);
    FileOutStream os = sFileSystem.createFile(filePath,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // persist the file
    URIStatus status = sFileSystem.getStatus(filePath);
    return sJobMaster.run(new PersistConfig(pathStr, 1, true, status.getUfsPath()));
  }

  protected JobInfo waitForJobToFinish(final long jobId)
      throws InterruptedException, TimeoutException {
    return JobTestUtils.waitForJobStatus(sJobMaster, jobId, Status.COMPLETED);
  }
}
