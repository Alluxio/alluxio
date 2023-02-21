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

import static alluxio.master.file.scheduler.LoadTestUtils.fileWithBlockLocations;
import static alluxio.master.file.scheduler.LoadTestUtils.generateRandomBlockStatus;
import static alluxio.master.file.scheduler.LoadTestUtils.generateRandomFileInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.exception.runtime.UnauthenticatedRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.job.JobState;
import alluxio.master.job.LoadJob;
import alluxio.master.journal.JournalContext;
import alluxio.master.scheduler.Scheduler;
import alluxio.proto.journal.Job;
import alluxio.resource.CloseableResource;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public final class SchedulerTest {

  @BeforeClass
  public static void before() {
    AuthenticatedClientUser.set("user");
  }

  @AfterClass
  public static void after() {
    AuthenticatedClientUser.remove();
  }

  @Test
  public void testGetActiveWorkers() throws IOException {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    CloseableResource<BlockWorkerClient> blockWorkerClient = mock(CloseableResource.class);
    Scheduler scheduler = new Scheduler(fileSystemMaster, fileSystemContext);
    when(fileSystemMaster.getWorkerInfoList())
        .thenReturn(ImmutableList.of(
            new WorkerInfo().setId(1).setAddress(
                new WorkerNetAddress().setHost("worker1").setRpcPort(1234)),
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234))))
        .thenThrow(new UnavailableException("test"))
        .thenReturn(ImmutableList.of(
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234))))
        .thenReturn(ImmutableList.of(
            new WorkerInfo().setId(1).setAddress(
                new WorkerNetAddress().setHost("worker1").setRpcPort(1234)),
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234))));
    when(fileSystemContext.acquireBlockWorkerClient(any())).thenReturn(blockWorkerClient);
    assertEquals(0, scheduler
        .getActiveWorkers().size());
    scheduler.updateWorkers();
    assertEquals(2, scheduler
        .getActiveWorkers().size());
    scheduler.updateWorkers();
    assertEquals(2, scheduler
        .getActiveWorkers().size());
    scheduler.updateWorkers();
    assertEquals(1, scheduler
        .getActiveWorkers().size());
    scheduler.updateWorkers();
    assertEquals(2, scheduler
        .getActiveWorkers().size());
  }

  @Test
  public void testSubmit() throws Exception {
    String validLoadPath = "/path/to/load";
    String invalidLoadPath = "/path/to/invalid";
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fileSystemMaster.createJournalContext()).thenReturn(journalContext);
    Scheduler scheduler = new Scheduler(fileSystemMaster, fileSystemContext);
    assertTrue(scheduler.submitJob(validLoadPath, OptionalLong.empty(), false, true));
    verify(journalContext).append(argThat(journalEntry -> journalEntry.hasLoadJob()
        && journalEntry.getLoadJob().getLoadPath().equals(validLoadPath)
        && journalEntry.getLoadJob().getState() == Job.PJobState.CREATED
        && !journalEntry.getLoadJob().hasBandwidth()
        && journalEntry.getLoadJob().getVerify()));
    assertEquals(1, scheduler
        .getLoadJobs().size());
    assertEquals(OptionalLong.empty(), scheduler
        .getLoadJobs().get(validLoadPath).getBandwidth());
    assertTrue(scheduler
        .getLoadJobs().get(validLoadPath).isVerificationEnabled());
    assertFalse(scheduler.submitJob(validLoadPath, OptionalLong.of(1000), true, false));
    verify(journalContext).append(argThat(journalEntry -> journalEntry.hasLoadJob()
        && journalEntry.getLoadJob().getLoadPath().equals(validLoadPath)
        && journalEntry.getLoadJob().getState() == Job.PJobState.CREATED
        && journalEntry.getLoadJob().getBandwidth() == 1000
        && !journalEntry.getLoadJob().getPartialListing()  // we don't update partialListing
        && !journalEntry.getLoadJob().getVerify()));
    assertEquals(1, scheduler
        .getLoadJobs().size());
    assertEquals(1000, scheduler
        .getLoadJobs().get(validLoadPath).getBandwidth().getAsLong());
    assertFalse(scheduler
        .getLoadJobs().get(validLoadPath).isVerificationEnabled());
    doThrow(new FileDoesNotExistException("test")).when(fileSystemMaster).checkAccess(any(), any());
    assertThrows(NotFoundRuntimeException.class,
        () -> scheduler.submitJob(invalidLoadPath, OptionalLong.empty(), false, true));
    doThrow(new InvalidPathException("test")).when(fileSystemMaster).checkAccess(any(), any());
    assertThrows(NotFoundRuntimeException.class,
        () -> scheduler.submitJob(invalidLoadPath, OptionalLong.empty(), false, true));
    doThrow(new AccessControlException("test")).when(fileSystemMaster).checkAccess(any(), any());
    assertThrows(UnauthenticatedRuntimeException.class,
        () -> scheduler.submitJob(invalidLoadPath, OptionalLong.empty(), false, true));
  }

  @Test
  public void testStop() throws Exception {
    String validLoadPath = "/path/to/load";
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fileSystemMaster.createJournalContext()).thenReturn(journalContext);
    Scheduler scheduler = new Scheduler(fileSystemMaster, fileSystemContext);
    assertTrue(scheduler.submitJob(validLoadPath, OptionalLong.of(100), false, true));
    verify(journalContext, times(1)).append(any());
    verify(journalContext).append(argThat(journalEntry -> journalEntry.hasLoadJob()
        && journalEntry.getLoadJob().getLoadPath().equals(validLoadPath)
        && journalEntry.getLoadJob().getState() == Job.PJobState.CREATED
        && journalEntry.getLoadJob().getBandwidth() == 100
        && journalEntry.getLoadJob().getVerify()));
    assertTrue(scheduler.stopLoad(validLoadPath));
    verify(journalContext, times(2)).append(any());
    verify(journalContext).append(argThat(journalEntry -> journalEntry.hasLoadJob()
        && journalEntry.getLoadJob().getLoadPath().equals(validLoadPath)
        && journalEntry.getLoadJob().getState() == Job.PJobState.STOPPED
        && journalEntry.getLoadJob().getBandwidth() == 100
        && journalEntry.getLoadJob().getVerify()
        && journalEntry.getLoadJob().hasEndTime()));
    assertFalse(scheduler.stopLoad(validLoadPath));
    verify(journalContext, times(2)).append(any());
    assertFalse(scheduler.stopLoad("/does/not/exist"));
    verify(journalContext, times(2)).append(any());
    assertFalse(scheduler.submitJob(validLoadPath, OptionalLong.of(100), false, true));
    verify(journalContext, times(3)).append(any());
    assertTrue(scheduler.stopLoad(validLoadPath));
    verify(journalContext, times(4)).append(any());
  }

  @Test
  public void testSubmitExceedsCapacity() throws Exception {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fileSystemMaster.createJournalContext()).thenReturn(journalContext);
    Scheduler scheduler = new Scheduler(fileSystemMaster, fileSystemContext);
    IntStream.range(0, 100).forEach(
        i -> assertTrue(scheduler.submitJob(
            String.format("/path/to/load/%d", i), OptionalLong.empty(), false, true)));
    assertThrows(
        ResourceExhaustedRuntimeException.class,
        () -> scheduler.submitJob("/path/to/load/101", OptionalLong.empty(), false, true));
  }

  @Test
  public void testScheduling() throws Exception {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fileSystemMaster.createJournalContext()).thenReturn(journalContext);
    CloseableResource<BlockWorkerClient> blockWorkerClientResource = mock(CloseableResource.class);
    BlockWorkerClient blockWorkerClient = mock(BlockWorkerClient.class);
    when(fileSystemMaster.getWorkerInfoList())
        .thenReturn(ImmutableList.of(
            new WorkerInfo().setId(1).setAddress(
                new WorkerNetAddress().setHost("worker1").setRpcPort(1234)),
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234))))
        .thenReturn(ImmutableList.of(
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234))))
        .thenReturn(ImmutableList.of(
            new WorkerInfo().setId(1).setAddress(
                new WorkerNetAddress().setHost("worker1").setRpcPort(1234)),
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234)),
            new WorkerInfo().setId(3).setAddress(
                new WorkerNetAddress().setHost("worker3").setRpcPort(1234)),
            new WorkerInfo().setId(4).setAddress(
                new WorkerNetAddress().setHost("worker4").setRpcPort(1234)),
            new WorkerInfo().setId(5).setAddress(
                new WorkerNetAddress().setHost("worker5").setRpcPort(1234)),
            new WorkerInfo().setId(6).setAddress(
                new WorkerNetAddress().setHost("worker6").setRpcPort(1234)),
            new WorkerInfo().setId(7).setAddress(
                new WorkerNetAddress().setHost("worker7").setRpcPort(1234)),
            new WorkerInfo().setId(8).setAddress(
                new WorkerNetAddress().setHost("worker8").setRpcPort(1234)),
            new WorkerInfo().setId(9).setAddress(
                new WorkerNetAddress().setHost("worker9").setRpcPort(1234)),
            new WorkerInfo().setId(10).setAddress(
                new WorkerNetAddress().setHost("worker10").setRpcPort(1234))));
    List<FileInfo> fileInfos = generateRandomFileInfo(100, 50, 64 * Constants.MB);
    when(fileSystemMaster.listStatus(any(), any()))
        .thenReturn(fileInfos)
        .thenReturn(fileWithBlockLocations(fileInfos, 0.95))
        .thenReturn(fileWithBlockLocations(fileInfos, 1.1));
    int failureRequestIteration = 50;
    int exceptionRequestIteration = 70;
    AtomicInteger iteration = new AtomicInteger();

    when(fileSystemContext.acquireBlockWorkerClient(any())).thenReturn(blockWorkerClientResource);
    when(blockWorkerClientResource.get()).thenReturn(blockWorkerClient);
    when(blockWorkerClient.load(any())).thenAnswer(invocation -> {
      iteration.getAndIncrement();
      LoadRequest request = invocation.getArgument(0);
      List<BlockStatus> status;
      if (iteration.get() == exceptionRequestIteration) {
        // Test worker exception
        SettableFuture<LoadResponse> responseFuture = SettableFuture.create();
        responseFuture.setException(new TimeoutException());
        return responseFuture;
      }
      else if (iteration.get() == failureRequestIteration) {
        // Test worker failing the whole request
        status = generateRandomBlockStatus(request.getBlocksList(), 1);
      }
      else {
        status = generateRandomBlockStatus(request.getBlocksList(), 0.01);
      }
      LoadResponse.Builder response = LoadResponse.newBuilder();
      if (status.stream().allMatch(s -> s.getCode() == Status.OK.getCode().value())) {
        response.setStatus(TaskStatus.SUCCESS);
      }
      else if (status.stream().allMatch(s -> s.getCode() != Status.OK.getCode().value())) {
        response.setStatus(TaskStatus.FAILURE)
            .addAllBlockStatus(status);
      }
      else {
        response.setStatus(TaskStatus.PARTIAL_FAILURE)
            .addAllBlockStatus(status.stream()
                .filter(s -> s.getCode() != Status.OK.getCode().value())
                .collect(ImmutableList.toImmutableList()));
      }
      SettableFuture<LoadResponse> responseFuture = SettableFuture.create();
      responseFuture.set(response.build());
      return responseFuture;
    });

    Scheduler scheduler = new Scheduler(fileSystemMaster, fileSystemContext);
    LoadJob loadJob = new LoadJob("test", Optional.of("user"), "1",
        OptionalLong.of(1000), false, true);
    scheduler.submitJob(loadJob);
    verify(journalContext).append(argThat(journalEntry -> journalEntry.hasLoadJob()
        && journalEntry.getLoadJob().getLoadPath().equals("test")
        && journalEntry.getLoadJob().getState() == Job.PJobState.CREATED
        && journalEntry.getLoadJob().getBandwidth() == 1000
        && journalEntry.getLoadJob().getVerify()));
    scheduler.start();
    while (!scheduler
        .getLoadProgress("test", JobProgressReportFormat.TEXT, false)
        .contains("SUCCEEDED")) {
      assertFalse(scheduler.submitJob(
          new LoadJob("test", Optional.of("user"), "1", OptionalLong.of(1000), false, true)));
      Thread.sleep(1000);
    }
    Thread.sleep(1000);
    scheduler.stop();
    assertEquals(JobState.SUCCEEDED, loadJob.getJobState());
    assertEquals(0, loadJob.getCurrentBlockCount());
    assertTrue(loadJob.getTotalBlockCount() > 5000);
    verify(journalContext).append(argThat(journalEntry -> journalEntry.hasLoadJob()
        && journalEntry.getLoadJob().getLoadPath().equals("test")
        && journalEntry.getLoadJob().getState() == Job.PJobState.SUCCEEDED
        && journalEntry.getLoadJob().getBandwidth() == 1000
        && journalEntry.getLoadJob().getVerify()));
    assertTrue(scheduler.submitJob(new LoadJob("test", "user", OptionalLong.of(1000))));
  }

  @Test
  public void testSchedulingFullCapacity() throws Exception {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fileSystemMaster.createJournalContext()).thenReturn(journalContext);
    CloseableResource<BlockWorkerClient> blockWorkerClientResource = mock(CloseableResource.class);
    BlockWorkerClient blockWorkerClient = mock(BlockWorkerClient.class);
    ImmutableList.Builder<WorkerInfo> workerInfos = ImmutableList.builder();
    for (int i = 0; i < 1000; i++) {
      workerInfos.add(new WorkerInfo().setId(i).setAddress(
          new WorkerNetAddress().setHost("worker" + i).setRpcPort(1234)));
    }
    when(fileSystemMaster.getWorkerInfoList())
        .thenReturn(workerInfos.build());
    List<FileInfo> fileInfos = generateRandomFileInfo(2000, 50, 64 * Constants.MB);
    when(fileSystemMaster.listStatus(any(), any()))
        .thenReturn(fileInfos);

    when(fileSystemContext.acquireBlockWorkerClient(any())).thenReturn(blockWorkerClientResource);
    when(blockWorkerClientResource.get()).thenReturn(blockWorkerClient);
    when(blockWorkerClient.load(any())).thenAnswer(invocation -> {
      LoadResponse.Builder response = LoadResponse.newBuilder().setStatus(TaskStatus.SUCCESS);
      SettableFuture<LoadResponse> responseFuture = SettableFuture.create();
      responseFuture.set(response.build());
      return responseFuture;
    });

    Scheduler scheduler = new Scheduler(fileSystemMaster, fileSystemContext);
    for (int i = 0; i < 100; i++) {
      LoadJob loadJob = new LoadJob("test" + i, "user", OptionalLong.of(1000));
      scheduler.submitJob(loadJob);
    }
    assertThrows(ResourceExhaustedRuntimeException.class,
        () -> scheduler.submitJob(new LoadJob("/way/too/many", "user", OptionalLong.empty())));
    scheduler.start();
    while (scheduler
        .getLoadJobs().values().stream()
        .anyMatch(loadJob -> loadJob.getJobState() != JobState.SUCCEEDED)) {
      Thread.sleep(1000);
    }
    scheduler.stop();
  }

  @Test
  public void testSchedulingWithException() throws Exception {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fileSystemMaster.createJournalContext()).thenReturn(journalContext);
    CloseableResource<BlockWorkerClient> blockWorkerClientResource = mock(CloseableResource.class);
    BlockWorkerClient blockWorkerClient = mock(BlockWorkerClient.class);
    when(fileSystemMaster.getWorkerInfoList())
        .thenReturn(ImmutableList.of(
            new WorkerInfo().setId(1).setAddress(
                new WorkerNetAddress().setHost("worker1").setRpcPort(1234)),
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234))));
    when(fileSystemContext.acquireBlockWorkerClient(any())).thenReturn(blockWorkerClientResource);
    when(blockWorkerClientResource.get()).thenReturn(blockWorkerClient);
    List<FileInfo> fileInfos = generateRandomFileInfo(100, 10, 64 * Constants.MB);
    when(fileSystemMaster.listStatus(any(), any()))
        // Non-retryable exception, first load job should fail
        .thenThrow(AccessControlException.class)
        // Retryable exception, second load job should succeed
        .thenThrow(new ResourceExhaustedRuntimeException("test", true))
        .thenReturn(fileInfos);
    Scheduler scheduler = new Scheduler(fileSystemMaster, fileSystemContext);
    scheduler.start();
    scheduler.submitJob("test", OptionalLong.of(1000), false, false);
    while (!scheduler
        .getLoadProgress("test", JobProgressReportFormat.TEXT, false)
        .contains("FAILED")) {
      Thread.sleep(1000);
    }
    when(blockWorkerClient.load(any())).thenAnswer(invocation -> {
      LoadResponse.Builder response = LoadResponse.newBuilder().setStatus(TaskStatus.SUCCESS);
      SettableFuture<LoadResponse> responseFuture = SettableFuture.create();
      responseFuture.set(response.build());
      return responseFuture;
    });
    scheduler.submitJob("test", OptionalLong.of(1000), false, false);
    while (!scheduler
        .getLoadProgress("test", JobProgressReportFormat.TEXT, false)
        .contains("SUCCEEDED")) {
      Thread.sleep(1000);
    }
    scheduler.stop();
  }

  @Test
  public void testJobRetention() throws Exception {
    Configuration.modifiableGlobal().set(PropertyKey.JOB_RETENTION_TIME, "0ms", Source.RUNTIME);
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fileSystemMaster.createJournalContext()).thenReturn(journalContext);
    Scheduler scheduler = new Scheduler(fileSystemMaster, fileSystemContext);
    scheduler.start();
    IntStream.range(0, 5).forEach(
        i -> assertTrue(scheduler.submitJob(
            String.format("/load/%d", i), OptionalLong.empty(), false, true)));
    assertEquals(5, scheduler
        .getLoadJobs().size());
    scheduler
        .getLoadJobs().get("/load/1").setJobState(JobState.VERIFYING);
    scheduler
        .getLoadJobs().get("/load/2").setJobState(JobState.FAILED);
    scheduler
        .getLoadJobs().get("/load/3").setJobState(JobState.SUCCEEDED);
    scheduler
        .getLoadJobs().get("/load/4").setJobState(JobState.STOPPED);
    scheduler.cleanupStaleJob();
    assertEquals(2, scheduler
        .getLoadJobs().size());
    assertTrue(scheduler
        .getLoadJobs().containsKey("/load/0"));
    assertTrue(scheduler
        .getLoadJobs().containsKey("/load/1"));
    IntStream.range(2, 5).forEach(
        i -> assertFalse(scheduler
            .getLoadJobs().containsKey(String.format("/load/%d", i))));
    Configuration.modifiableGlobal().unset(PropertyKey.JOB_RETENTION_TIME);
  }
}
