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

package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.answersWithDelay;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.UnderFileSystemFactoryRegistryRule;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.grpc.WritePType;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.Status;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemFactory;
import alluxio.testutils.underfs.sleeping.SleepingUnderFileSystemOptions;
import alluxio.util.io.PathUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.internal.stubbing.answers.Returns;

/**
 * Tests for cross-mount {@link alluxio.cli.fs.command.DistributedCpCommand}.
 */
public final class DistributedCpCancelTest extends AbstractFileSystemShellTest {
  private static final long SLEEP_MS = Constants.SECOND_MS * 60;
  private static final int TEST_TIMEOUT = 15;
  @ClassRule
  public static UnderFileSystemFactoryRegistryRule sUnderfilesystemfactoryregistry =
      new UnderFileSystemFactoryRegistryRule(new SleepingUnderFileSystemFactory(
          new SleepingUnderFileSystemOptions()
              .setIsDirectoryMs(SLEEP_MS)
              .setIsFileMs(SLEEP_MS)
              .setGetStatusMs(SLEEP_MS)
              .setListStatusMs(SLEEP_MS)
              .setListStatusWithOptionsMs(SLEEP_MS)
              .setExistsMs(SLEEP_MS)
              .setMkdirsMs(SLEEP_MS)));
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void testDistributedCpCancelStats() throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFileSource", WritePType.THROUGH, 10);

    long jobId = sJobMaster.run(new MigrateConfig(
        "/testFileSource", "/testFileDest", "THROUGH", false));
    try (MockedStatic<IOUtils> utilities = Mockito.mockStatic(IOUtils.class)) {
      utilities.when(()->IOUtils.copyLarge(any(FileInStream.class),any())).thenAnswer(answersWithDelay(1000, new Returns(1L)));
    }
    sJobShell.run("cancel", Long.toString(jobId));

    JobTestUtils
        .waitForJobStatus(sJobMaster, jobId, Sets.newHashSet(Status.CANCELED), TEST_TIMEOUT);
    Assert.assertTrue(sFileSystem.exists(new AlluxioURI("/testFileDest")));
  }
}
