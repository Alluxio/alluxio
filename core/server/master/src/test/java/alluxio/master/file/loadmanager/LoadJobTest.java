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

package alluxio.master.file.loadmanager;

import static alluxio.master.file.loadmanager.LoadTestUtils.generateRandomFileInfo;
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
import alluxio.grpc.LoadProgressReportFormat;
import alluxio.master.file.FileSystemMaster;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

public class LoadJobTest {
  @Test
  public void testGetNextBatch()
      throws FileDoesNotExistException, AccessControlException, IOException, InvalidPathException {
    List<FileInfo> fileInfos = generateRandomFileInfo(5, 20, 64 * Constants.MB);
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    when(fileSystemMaster.listStatus(any(), any()))
        .thenReturn(fileInfos);
    LoadJob load = new LoadJob("test", "user", OptionalLong.empty());
    List<Block> batch = load.getNextBatch(fileSystemMaster, 10);
    assertEquals(10, batch.size());
    assertEquals(1, batch.stream().map(Block::getUfsPath).distinct().count());

    batch.forEach(load::addBlockToRetry);

    batch = load.getNextBatch(fileSystemMaster, 80);
    assertEquals(80, batch.size());
    assertEquals(5, batch.stream().map(Block::getUfsPath).distinct().count());

    batch = load.getNextBatch(fileSystemMaster, 80);
    assertEquals(10, batch.size());
    assertEquals(1, batch.stream().map(Block::getUfsPath).distinct().count());

    batch = load.getNextBatch(fileSystemMaster, 80);
    assertEquals(10, batch.size());
    assertEquals(1, batch.stream().map(Block::getUfsPath).distinct().count());
    assertEquals(ImmutableSet.of(fileInfos.get(0).getUfsPath()),
        batch.stream().map(Block::getUfsPath).collect(ImmutableSet.toImmutableSet()));

    batch = load.getNextBatch(fileSystemMaster, 80);
    assertEquals(0, batch.size());
  }

  @Test
  public void testIsHealthy()
      throws FileDoesNotExistException, AccessControlException, IOException, InvalidPathException {
    List<FileInfo> fileInfos = generateRandomFileInfo(100, 5, 64 * 1024 * 1024);
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    when(fileSystemMaster.listStatus(any(), any()))
        .thenReturn(fileInfos);
    LoadJob loadJob = new LoadJob("test", "user", OptionalLong.empty());
    List<Block> batch = loadJob.getNextBatch(fileSystemMaster, 100);
    assertTrue(loadJob.isHealthy());
    loadJob.getNextBatch(fileSystemMaster, 100);
    assertTrue(loadJob.isHealthy());
    batch.forEach(loadJob::addBlockToRetry);
    assertTrue(loadJob.isHealthy());
    batch = loadJob.getNextBatch(fileSystemMaster, 100);
    assertTrue(loadJob.isHealthy());
    batch.forEach(loadJob::addBlockToRetry);
    assertFalse(loadJob.isHealthy());
  }

  @Test
  public void testLoadProgressReport() throws Exception {
    List<FileInfo> fileInfos = generateRandomFileInfo(10, 10, 64 * Constants.MB);
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    when(fileSystemMaster.listStatus(any(), any()))
        .thenReturn(fileInfos);
    LoadJob job = spy(new LoadJob("/test", "user", OptionalLong.empty()));
    when(job.getDurationInSec()).thenReturn(0L);
    job.setJobState(LoadJobState.LOADING);
    List<Block> blocks = job.getNextBatch(fileSystemMaster, 25);
    job.addLoadedBytes(640 * Constants.MB);
    String expectedTextReport = "\tSettings:\tbandwidth: unlimited\tverify: false\n"
        + "\tJob State: LOADING\n"
        + "\tFiles Processed: 3 out of 10\n"
        + "\tBytes Loaded: 640.00MB out of 6.25GB\n"
        + "\tBlock load failure rate: 0.00%\n"
        + "\tFiles Failed: 0\n";
    assertEquals(expectedTextReport, job.getProgress(LoadProgressReportFormat.TEXT, false));
    assertEquals(expectedTextReport, job.getProgress(LoadProgressReportFormat.TEXT, true));
    String expectedJsonReport = "{\"mVerbose\":false,\"mJobState\":\"LOADING\","
        + "\"mVerificationEnabled\":false,\"mProcessedFileCount\":3,\"mTotalFileCount\":10,"
        + "\"mLoadedByteCount\":671088640,\"mTotalByteCount\":6710886400,"
        + "\"mFailurePercentage\":0.0,\"mFailedFileCount\":0}";
    assertEquals(expectedJsonReport, job.getProgress(LoadProgressReportFormat.JSON, false));
    job.addBlockFailure(blocks.get(0), "Test error 1", 2);
    job.addBlockFailure(blocks.get(4), "Test error 2", 2);
    job.addBlockFailure(blocks.get(10),  "Test error 3", 2);
    job.failJob(new InternalRuntimeException("test"));
    String expectedTextReportWithError = "\tSettings:\tbandwidth: unlimited\tverify: false\n"
        + "\tJob State: FAILED (alluxio.exception.runtime.InternalRuntimeException: test)\n"
        + "\tFiles Processed: 3 out of 10\n"
        + "\tBytes Loaded: 640.00MB out of 6.25GB\n"
        + "\tBlock load failure rate: 12.00%\n"
        + "\tFiles Failed: 2\n";
    assertEquals(expectedTextReportWithError,
        job.getProgress(LoadProgressReportFormat.TEXT, false));
    String textReport = job.getProgress(LoadProgressReportFormat.TEXT, true);
    assertFalse(textReport.contains("Test error 1"));
    assertTrue(textReport.contains("Test error 2"));
    assertTrue(textReport.contains("Test error 3"));
    String jsonReport = job.getProgress(LoadProgressReportFormat.JSON, false);
    assertTrue(jsonReport.contains("FAILED"));
    assertTrue(jsonReport.contains("mFailureReason"));
    assertFalse(jsonReport.contains("Test error 2"));
    jsonReport = job.getProgress(LoadProgressReportFormat.JSON, true);
    assertFalse(jsonReport.contains("Test error 1"));
    assertTrue(jsonReport.contains("Test error 2"));
    assertTrue(jsonReport.contains("Test error 3"));
  }
}
