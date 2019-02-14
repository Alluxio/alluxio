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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.time.ExponentialTimer;
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for {@link PersistJob}.
 */
public final class PersistJobTest {

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long jobId = random.nextLong();
    long fileId = random.nextLong();
    AlluxioURI uri = new AlluxioURI(CommonUtils.randomAlphaNumString(random.nextInt(10)));
    String tempUfsPath = CommonUtils.randomAlphaNumString(random.nextInt(10));
    ExponentialTimer timer = new ExponentialTimer(0, 0, 0, 0);
    PersistJob.CancelState cancelState =
        PersistJob.CancelState.values()[random.nextInt(PersistJob.CancelState.values().length)];

    PersistJob persistJob = new PersistJob(jobId, fileId, uri, tempUfsPath, timer);
    persistJob.setCancelState(cancelState);

    Assert.assertEquals(jobId, persistJob.getId());
    Assert.assertEquals(fileId, persistJob.getFileId());
    Assert.assertEquals(uri, persistJob.getUri());
    Assert.assertEquals(tempUfsPath, persistJob.getTempUfsPath());
    Assert.assertEquals(timer, persistJob.getTimer());
    Assert.assertEquals(cancelState, persistJob.getCancelState());
  }
}
