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

package alluxio.master.file.scheduler;

import static alluxio.master.file.scheduler.LoadTestUtils.generateRandomFileInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.grpc.Block;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.job.FileIterable;
import alluxio.master.job.LoadJob;
import alluxio.scheduler.job.JobState;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public class LoadJobTest {
  @Test
  public void testGetNextBatch()
      throws FileDoesNotExistException, AccessControlException, IOException, InvalidPathException {
    List<FileInfo> fileInfos = generateRandomFileInfo(5, 20, 64 * Constants.MB);

    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    when(fileSystemMaster.listStatus(any(), any())).thenReturn(fileInfos);
    String testPath = "test";
    Optional<String> user = Optional.of("user");
    FileIterable files =
        new FileIterable(fileSystemMaster, testPath, user, false,
            LoadJob.QUALIFIED_FILE_FILTER);
    LoadJob load =
        new LoadJob(testPath, user, "1", OptionalLong.empty(), false, false, files);
    List<Block> batch = load.getNextBatchBlocks(10);
    assertEquals(10, batch.size());
    assertEquals(1, batch.stream().map(Block::getUfsPath).distinct().count());

    batch.forEach(load::addBlockToRetry);

    batch = load.getNextBatchBlocks(80);
    assertEquals(80, batch.size());
    assertEquals(5, batch.stream().map(Block::getUfsPath).distinct().count());

    batch = load.getNextBatchBlocks(80);
    assertEquals(10, batch.size());
    assertEquals(1, batch.stream().map(Block::getUfsPath).distinct().count());

    batch = load.getNextBatchBlocks(80);
    assertEquals(10, batch.size());
    assertEquals(1, batch.stream().map(Block::getUfsPath).distinct().count());
    assertEquals(ImmutableSet.of(fileInfos.get(0).getUfsPath()),
        batch.stream().map(Block::getUfsPath).collect(ImmutableSet.toImmutableSet()));

    batch = load.getNextBatchBlocks(80);
    assertEquals(0, batch.size());
  }

  @Test
  public void testGetNextBatchWithPartialListing()
      throws FileDoesNotExistException, AccessControlException, IOException, InvalidPathException {
    List<FileInfo> fileInfos = generateRandomFileInfo(400, 2, 64 * Constants.MB);

    for (int i = 0; i < 100; i++) {
      fileInfos.get(i).setInAlluxioPercentage(100);
    }
    for (int i = 200; i < 300; i++) {
      fileInfos.get(i).setInAlluxioPercentage(100);
    }
    for (int i = 0; i < 10; i++) {
      fileInfos.get(300 + i * i).setInAlluxioPercentage(100);
    }

    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    when(fileSystemMaster.listStatus(any(), any())).thenAnswer(invocation -> {
      ListStatusContext context = invocation.getArgument(1, ListStatusContext.class);
      int fileSize = fileInfos.size();
      int from = 0;
      int to = fileSize;
      if (context.isPartialListing()) {
        String startAfter = context.getPartialOptions().get().getStartAfter();
        int batch = context.getPartialOptions().get().getBatchSize();
        for (int i = 0; i < fileSize; i++) {
          if (startAfter.equals(fileInfos.get(i).getPath())) {
            from = i + 1;
            break;
          }
        }
        to = fileSize < from + batch ? fileSize : from + batch;
      }
      return fileInfos.subList(from, to);
    });
    String testPath = "test";
    Optional<String> user = Optional.of("user");
    FileIterable files =
        new FileIterable(fileSystemMaster, testPath, user, true,
            LoadJob.QUALIFIED_FILE_FILTER);
    LoadJob load =
        new LoadJob(testPath, user, "1", OptionalLong.empty(), true, false, files);

    List<Block> batch = load.getNextBatchBlocks(100);
    assertEquals(100, batch.size());
    assertEquals(50, batch.stream().map(Block::getUfsPath).distinct().count());

    batch = load.getNextBatchBlocks(200);
    assertEquals(200, batch.size());
    assertEquals(100, batch.stream().map(Block::getUfsPath).distinct().count());

    batch = load.getNextBatchBlocks(300);
    assertEquals(80, batch.size());
    assertEquals(40, batch.stream().map(Block::getUfsPath).distinct().count());

    batch = load.getNextBatchBlocks(100);
    assertEquals(0, batch.size());
  }

  @Test
  public void testIsHealthy()
      throws FileDoesNotExistException, AccessControlException, IOException, InvalidPathException {
    List<FileInfo> fileInfos = generateRandomFileInfo(100, 5, 64 * 1024 * 1024);
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    when(fileSystemMaster.listStatus(any(), any())).thenReturn(fileInfos);
    FileIterable files = new FileIterable(fileSystemMaster, "test", Optional.of("user"), false,
        LoadJob.QUALIFIED_FILE_FILTER);
    LoadJob loadJob =
        new LoadJob("test", Optional.of("user"), "1", OptionalLong.empty(), false, false, files);
    List<Block> batch = loadJob.getNextBatchBlocks(100);
    assertTrue(loadJob.isHealthy());
    loadJob.getNextBatchBlocks(100);
    assertTrue(loadJob.isHealthy());
    batch.forEach(loadJob::addBlockToRetry);
    assertTrue(loadJob.isHealthy());
    batch = loadJob.getNextBatchBlocks(100);
    assertTrue(loadJob.isHealthy());
    batch.forEach(loadJob::addBlockToRetry);
    assertFalse(loadJob.isHealthy());
  }

  @Test
  public void testLoadProgressReport() throws Exception {
    List<FileInfo> fileInfos = generateRandomFileInfo(10, 10, 64 * Constants.MB);
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    when(fileSystemMaster.listStatus(any(), any())).thenReturn(fileInfos);
    FileIterable files = new FileIterable(fileSystemMaster, "test", Optional.of("user"), false,
        LoadJob.QUALIFIED_FILE_FILTER);
    LoadJob job =
        spy(new LoadJob("test", Optional.of("user"), "1", OptionalLong.empty(), false, false,
            files));
    when(job.getDurationInSec()).thenReturn(0L);
    job.setJobState(JobState.RUNNING);
    List<Block> blocks = job.getNextBatchBlocks(25);
    job.addLoadedBytes(640 * Constants.MB);
    String expectedTextReport = "\tSettings:\tbandwidth: unlimited\tverify: false\n"
        + "\tJob State: RUNNING\n"
        + "\tFiles Processed: 3\n"
        + "\tBytes Loaded: 640.00MB out of 1600.00MB\n"
        + "\tBlock load failure rate: 0.00%\n"
        + "\tFiles Failed: 0\n";
    assertEquals(expectedTextReport, job.getProgress(JobProgressReportFormat.TEXT, false));
    assertEquals(expectedTextReport, job.getProgress(JobProgressReportFormat.TEXT, true));
    String expectedJsonReport = "{\"mVerbose\":false,\"mJobState\":\"RUNNING\","
        + "\"mVerificationEnabled\":false,\"mProcessedFileCount\":3,"
        + "\"mLoadedByteCount\":671088640,\"mTotalByteCount\":1677721600,"
        + "\"mFailurePercentage\":0.0,\"mFailedFileCount\":0}";
    assertEquals(expectedJsonReport, job.getProgress(JobProgressReportFormat.JSON, false));
    job.addBlockFailure(blocks.get(0), "Test error 1", 2);
    job.addBlockFailure(blocks.get(4), "Test error 2", 2);
    job.addBlockFailure(blocks.get(10),  "Test error 3", 2);
    job.failJob(new InternalRuntimeException("test"));
    String expectedTextReportWithError = "\tSettings:\tbandwidth: unlimited\tverify: false\n"
        + "\tJob State: FAILED (alluxio.exception.runtime.InternalRuntimeException: test)\n"
        + "\tFiles Processed: 3\n"
        + "\tBytes Loaded: 640.00MB out of 1600.00MB\n"
        + "\tBlock load failure rate: 12.00%\n"
        + "\tFiles Failed: 2\n";
    assertEquals(expectedTextReportWithError,
        job.getProgress(JobProgressReportFormat.TEXT, false));
    String textReport = job.getProgress(JobProgressReportFormat.TEXT, true);
    assertFalse(textReport.contains("Test error 1"));
    assertTrue(textReport.contains("Test error 2"));
    assertTrue(textReport.contains("Test error 3"));
    String jsonReport = job.getProgress(JobProgressReportFormat.JSON, false);
    assertTrue(jsonReport.contains("FAILED"));
    assertTrue(jsonReport.contains("mFailureReason"));
    assertFalse(jsonReport.contains("Test error 2"));
    jsonReport = job.getProgress(JobProgressReportFormat.JSON, true);
    assertFalse(jsonReport.contains("Test error 1"));
    assertTrue(jsonReport.contains("Test error 2"));
    assertTrue(jsonReport.contains("Test error 3"));
  }
}
