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

package alluxio.dora.client.cli.fs.command;

import alluxio.dora.AlluxioURI;
import alluxio.dora.Constants;
import alluxio.dora.UnderFileSystemFactoryRegistryRule;
import alluxio.dora.client.WriteType;
import alluxio.dora.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.dora.job.plan.migrate.MigrateConfig;
import alluxio.dora.job.util.JobTestUtils;
import alluxio.dora.job.wire.Status;
import alluxio.dora.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.dora.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;

/**
 * Tests for cancel functionality of {@link alluxio.cli.fs.command.DistributedCpCommand}.
 */
public final class DistributedCpCancelTest extends AbstractFileSystemShellTest {
  private static final long SLEEP_MS = Constants.SECOND_MS * 30;
  private static final int TEST_TIMEOUT = Constants.SECOND_MS * 90;

  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new SleepingUnderFileSystemFactory(
          new SleepingUnderFileSystemOptions()
              .setOpenMs(SLEEP_MS)));
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    String localUfsPath = mTempFolder.getRoot().getAbsolutePath();
    sFileSystem.mount(new AlluxioURI("/mnt/"), new AlluxioURI("sleep://" + localUfsPath));
    new File(localUfsPath + "/dir/").mkdirs();
    FileWriter fileWriter = new FileWriter(localUfsPath + "/dir/file");
    fileWriter.write("test");
    fileWriter.close();
  }

  @Test
  public void testDistributedCpCancelStats() throws Exception {

    long jobId = sJobMaster.run(new MigrateConfig(
        "/mnt/dir/file", "/testFileDest", WriteType.THROUGH, false));
    // wait for execution until blocked by IOUtils.copyLarge() in migration job
    Thread.sleep(Constants.SECOND_MS * 3);
    sJobShell.run("cancel", Long.toString(jobId));
    JobTestUtils
        .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED), TEST_TIMEOUT);
    Assert.assertFalse(sFileSystem.exists(new AlluxioURI("/testFileDest")));
  }
}
