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

import static alluxio.master.file.scheduler.JobTestUtils.fileWithBlockLocations;
import static alluxio.master.file.scheduler.JobTestUtils.generateRandomBlockStatus;
import static alluxio.master.file.scheduler.JobTestUtils.generateRandomFileInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AccessControlException;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.job.JobDescription;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.job.DoraLoadJob;
import alluxio.master.job.FileIterable;
import alluxio.master.job.LoadJob;
import alluxio.master.journal.JournalContext;
import alluxio.master.scheduler.DefaultWorkerProvider;
import alluxio.master.scheduler.JournaledJobMetaStore;
import alluxio.master.scheduler.Scheduler;
import alluxio.proto.journal.Job;
import alluxio.resource.CloseableResource;
import alluxio.scheduler.job.JobMetaStore;
import alluxio.scheduler.job.JobState;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class SchedulerTest {

  @BeforeClass
  public static void before() {
    AuthenticatedClientUser.set("user");
    Configuration.reloadProperties();
  }

  @AfterClass
  public static void after() {
    AuthenticatedClientUser.remove();
  }

  @Before
  public void beforeTest() {
    Configuration.reloadProperties();
    Configuration.set(PropertyKey.MASTER_SCHEDULER_INITIAL_DELAY, "1s");
  }

  @Test
  public void testGetActiveWorkers() throws IOException {
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    CloseableResource<BlockWorkerClient> blockWorkerClient = mock(CloseableResource.class);
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider,
        new InMemoryJobMetaStore());
    when(fsMaster.getWorkerInfoList())
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
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    AlluxioProperties alluxioProperties = new AlluxioProperties();
    InstancedConfiguration conf = new InstancedConfiguration(alluxioProperties);
    when(fileSystemContext.getClusterConf()).thenReturn(conf);

    JournalContext journalContext = mock(JournalContext.class);
    when(fsMaster.createJournalContext()).thenReturn(journalContext);
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    InMemoryJobMetaStore jobMetaStore = new InMemoryJobMetaStore();
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider,
        jobMetaStore);
    DoraLoadJob loadJob =
        new DoraLoadJob(validLoadPath, Optional.of("user"), "1", OptionalLong.empty(),
            false, true, false);
    assertTrue(scheduler.submitJob(loadJob));
    assertTrue(jobMetaStore.get(loadJob.getJobId()).getJobState() == JobState.RUNNING);
    assertEquals(1, scheduler.getJobs().size());

    // Verify the job present in Scheduler and jobMetaStore has been updated.
    final DoraLoadJob loadJobFinal = loadJob;
    Optional<alluxio.scheduler.job.Job<?>> loadJobInMetaStore =
        scheduler.getJobMetaStore().getJobs().stream()
        .filter(j -> j.equals(loadJobFinal)).findFirst();
    assertTrue(loadJobInMetaStore.isPresent());
    assertEquals(OptionalLong.empty(), ((DoraLoadJob) loadJobInMetaStore.get())
        .getBandwidth());
    DoraLoadJob job = (DoraLoadJob) scheduler.getJobs().get(loadJob.getDescription());
    assertEquals(OptionalLong.empty(), job.getBandwidth());
    loadJob =
        new DoraLoadJob(validLoadPath, Optional.of("user"), "1",
            OptionalLong.of(1000), true, false, false);
    assertFalse(scheduler.submitJob(loadJob));
    assertEquals(1, scheduler.getJobs().size());
    job = (DoraLoadJob) scheduler.getJobs().get(loadJob.getDescription());
    assertFalse(job.getBandwidth().isPresent());

    // Verify the job present in Scheduler and jobMetaStore
    final DoraLoadJob loadJobFinalNew = loadJob;
    Optional<alluxio.scheduler.job.Job<?>> loadJobInMetaStoreNewBandwidth =
        scheduler.getJobMetaStore().getJobs().stream()
            .filter(j -> j.equals(loadJobFinalNew)).findFirst();
    assertTrue(loadJobInMetaStore.isPresent());
    assertFalse(((DoraLoadJob) loadJobInMetaStoreNewBandwidth.get())
        .getBandwidth().isPresent());
  }

  @Test
  public void testStop() {
    String validLoadPath = "/path/to/load";
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    InMemoryJobMetaStore metaStore = new InMemoryJobMetaStore();
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider, metaStore);
    DoraLoadJob job =
        new DoraLoadJob(validLoadPath, Optional.of("user"), "3", OptionalLong.of(100),
            false, true, false);
    assertTrue(scheduler.submitJob(job));

    assertTrue(scheduler.stopJob(job.getDescription()));

    assertTrue(metaStore.get(job.getJobId()).getJobState() == JobState.STOPPED);
    assertFalse(scheduler.stopJob(job.getDescription()));
    assertFalse(scheduler.stopJob(JobDescription.newBuilder().setPath("/does/not/exist").build()));
    assertFalse(scheduler.submitJob(job));
    assertTrue(metaStore.get(job.getJobId()).getJobState() == JobState.RUNNING);
    assertTrue(scheduler.stopJob(job.getDescription()));
    assertTrue(metaStore.get(job.getJobId()).getJobState() == JobState.STOPPED);
  }

  @Test
  public void testSubmitExceedsCapacity() throws Exception {
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    AlluxioProperties alluxioProperties = new AlluxioProperties();
    InstancedConfiguration conf = new InstancedConfiguration(alluxioProperties);
    when(fileSystemContext.getClusterConf()).thenReturn(conf);

    JournalContext journalContext = mock(JournalContext.class);
    when(fsMaster.createJournalContext()).thenReturn(journalContext);
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider,
        new JournaledJobMetaStore(fsMaster));
    IntStream.range(0, 100).forEach(
        i -> {
          String path = String.format("/path/to/load/%d", i);
          assertTrue(scheduler.submitJob(
              new DoraLoadJob(path, Optional.of("user"), "1", OptionalLong.empty(),
                  false, true, false)
          ));
        });
    assertThrows(ResourceExhaustedRuntimeException.class, () -> scheduler.submitJob(
        new DoraLoadJob("/path/to/load/101", Optional.of("user"), "1", OptionalLong.empty(), false,
            true, false)));
  }

  @Ignore
  @Test
  public void testScheduling() throws Exception {
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    AlluxioProperties alluxioProperties = new AlluxioProperties();
    InstancedConfiguration conf = new InstancedConfiguration(alluxioProperties);
    when(fileSystemContext.getClusterConf()).thenReturn(conf);
    JournalContext journalContext = mock(JournalContext.class);
    when(fsMaster.createJournalContext()).thenReturn(journalContext);
    CloseableResource<BlockWorkerClient> blockWorkerClientResource = mock(CloseableResource.class);
    BlockWorkerClient blockWorkerClient = mock(BlockWorkerClient.class);
    when(fsMaster.getWorkerInfoList())
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
    when(fsMaster.listStatus(any(), any()))
        .thenReturn(fileInfos)
        .thenReturn(fileWithBlockLocations(fileInfos, 0.95))
        .thenReturn(fileWithBlockLocations(fileInfos, 1.1));
    when(fileSystemContext.acquireBlockWorkerClient(any())).thenReturn(blockWorkerClientResource);
    when(blockWorkerClientResource.get()).thenReturn(blockWorkerClient);
    AtomicInteger iteration = new AtomicInteger();
    when(blockWorkerClient.load(any())).thenAnswer(invocation -> {
      LoadRequest request = invocation.getArgument(0);
      return buildResponseFuture(request, iteration);
    });
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider,
        new JournaledJobMetaStore(fsMaster));
    String path = "test";
    FileIterable files = new FileIterable(fsMaster, path, Optional.of("user"), false,
        LoadJob.QUALIFIED_FILE_FILTER);
    LoadJob loadJob = new LoadJob(path, Optional.of("user"), "1",
        OptionalLong.of(1000), false, true, files);
    scheduler.submitJob(loadJob);
    verify(journalContext).append(argThat(journalEntry -> journalEntry.hasLoadJob()
        && journalEntry.getLoadJob().getLoadPath().equals(path)
        && journalEntry.getLoadJob().getState() == Job.PJobState.CREATED
        && journalEntry.getLoadJob().getBandwidth() == 1000
        && journalEntry.getLoadJob().getVerify()));

    scheduler.start();
    while (!scheduler
        .getJobProgress(loadJob.getDescription(), JobProgressReportFormat.TEXT, false)
        .contains("FAILED")) {
      assertFalse(scheduler.submitJob(
          new LoadJob(path, Optional.of("user"), "1", OptionalLong.of(1000), false, true, files)));
      Thread.sleep(1000);
    }
    Thread.sleep(1000);
    scheduler.stop();
    assertEquals(JobState.FAILED, loadJob.getJobState());
    assertEquals(0, loadJob.getCurrentBlockCount());
    verify(journalContext).append(argThat(journalEntry -> journalEntry.hasLoadJob()
        && journalEntry.getLoadJob().getLoadPath().equals(path)
        && journalEntry.getLoadJob().getState() == Job.PJobState.FAILED
        && journalEntry.getLoadJob().getBandwidth() == 1000
        && journalEntry.getLoadJob().getVerify()));
    assertTrue(scheduler.submitJob(new LoadJob(path, "user", OptionalLong.of(1000), files)));
  }

  private ListenableFuture<LoadResponse> buildResponseFuture(LoadRequest request,
      AtomicInteger iteration) {
    int failureRequestIteration = 50;
    int exceptionRequestIteration = 70;

    iteration.getAndIncrement();
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
  }

  @Test
  @Ignore
  public void testSchedulingFullCapacity() throws Exception {
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fsMaster.createJournalContext()).thenReturn(journalContext);
    CloseableResource<BlockWorkerClient> blockWorkerClientResource = mock(CloseableResource.class);
    BlockWorkerClient blockWorkerClient = mock(BlockWorkerClient.class);
    ImmutableList.Builder<WorkerInfo> workerInfos = ImmutableList.builder();
    for (int i = 0; i < 1000; i++) {
      workerInfos.add(new WorkerInfo().setId(i).setAddress(
          new WorkerNetAddress().setHost("worker" + i).setRpcPort(1234)));
    }
    when(fsMaster.getWorkerInfoList())
        .thenReturn(workerInfos.build());
    List<FileInfo> fileInfos = generateRandomFileInfo(2000, 50, 64 * Constants.MB);
    when(fsMaster.listStatus(any(), any()))
        .thenReturn(fileInfos);

    when(fileSystemContext.acquireBlockWorkerClient(any())).thenReturn(blockWorkerClientResource);
    when(blockWorkerClientResource.get()).thenReturn(blockWorkerClient);
    when(blockWorkerClient.load(any())).thenAnswer(invocation -> {
      LoadResponse.Builder response = LoadResponse.newBuilder().setStatus(TaskStatus.SUCCESS);
      SettableFuture<LoadResponse> responseFuture = SettableFuture.create();
      responseFuture.set(response.build());
      return responseFuture;
    });
    FileIterable files =
        new FileIterable(fsMaster, "test", Optional.of("user"), false,
            LoadJob.QUALIFIED_FILE_FILTER);
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider,
        new JournaledJobMetaStore(fsMaster));
    for (int i = 0; i < 100; i++) {
      LoadJob loadJob = new LoadJob("test" + i, "user", OptionalLong.of(1000), files);
      scheduler.submitJob(loadJob);
    }
    assertThrows(ResourceExhaustedRuntimeException.class, () -> scheduler.submitJob(
        new LoadJob("/way/too/many", "user", OptionalLong.empty(), files)));
    scheduler.start();
    while (scheduler
        .getJobs().values().stream()
        .anyMatch(loadJob -> loadJob.getJobState() != JobState.SUCCEEDED)) {
      Thread.sleep(1000);
    }
    scheduler.stop();
  }

  @Test
  @Ignore
  public void testSchedulingWithException() throws Exception {
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    JournalContext journalContext = mock(JournalContext.class);
    when(fsMaster.createJournalContext()).thenReturn(journalContext);
    CloseableResource<BlockWorkerClient> blockWorkerClientResource = mock(CloseableResource.class);
    BlockWorkerClient blockWorkerClient = mock(BlockWorkerClient.class);
    when(fsMaster.getWorkerInfoList())
        .thenReturn(ImmutableList.of(
            new WorkerInfo().setId(1).setAddress(
                new WorkerNetAddress().setHost("worker1").setRpcPort(1234)),
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234))));
    when(fileSystemContext.acquireBlockWorkerClient(any())).thenReturn(blockWorkerClientResource);
    when(blockWorkerClientResource.get()).thenReturn(blockWorkerClient);
    List<FileInfo> fileInfos = generateRandomFileInfo(100, 10, 64 * Constants.MB);
    when(fsMaster.listStatus(any(), any()))
        // Non-retryable exception, first load job should fail
        .thenThrow(AccessControlException.class)
        // Retryable exception, second load job should succeed
        .thenThrow(new ResourceExhaustedRuntimeException("test", true))
        .thenReturn(fileInfos);
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider,
        new JournaledJobMetaStore(fsMaster));
    scheduler.start();
    FileIterable files =
        new FileIterable(fsMaster, "test", Optional.of("user"), false,
            LoadJob.QUALIFIED_FILE_FILTER);
    LoadJob job = new LoadJob("test", "user", OptionalLong.of(1000), files);
    scheduler.submitJob(job);
    while (!scheduler
        .getJobProgress(job.getDescription(), JobProgressReportFormat.TEXT, false)
        .contains("FAILED")) {
      Thread.sleep(1000);
    }
    when(blockWorkerClient.load(any())).thenAnswer(invocation -> {
      LoadResponse.Builder response = LoadResponse.newBuilder().setStatus(TaskStatus.SUCCESS);
      SettableFuture<LoadResponse> responseFuture = SettableFuture.create();
      responseFuture.set(response.build());
      return responseFuture;
    });
    job = new LoadJob("test", "user", OptionalLong.of(1000), files);
    scheduler.submitJob(job);
    while (!scheduler
        .getJobProgress(job.getDescription(), JobProgressReportFormat.TEXT, false)
        .contains("SUCCEEDED")) {
      Thread.sleep(1000);
    }
    scheduler.stop();
  }

  @Test
  public void testJobRetention() throws Exception {
    Configuration.modifiableGlobal().set(PropertyKey.JOB_RETENTION_TIME, "0ms", Source.RUNTIME);
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    AlluxioProperties alluxioProperties = new AlluxioProperties();
    InstancedConfiguration conf = new InstancedConfiguration(alluxioProperties);
    when(fileSystemContext.getClusterConf()).thenReturn(conf);

    JournalContext journalContext = mock(JournalContext.class);
    when(fsMaster.createJournalContext()).thenReturn(journalContext);
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider,
        new JournaledJobMetaStore(fsMaster));
    scheduler.start();
    IntStream
        .range(0, 5)
        .forEach(i -> {
          String path = String.format("/load/%d", i);
          assertTrue(scheduler.submitJob(
              new DoraLoadJob(path, Optional.of("user"), "1",
                  OptionalLong.empty(), false, true, false)
          ));
        });
    assertEquals(5, scheduler
        .getJobs().size());
    scheduler
        .getJobs()
        .get(JobDescription
            .newBuilder()
            .setPath("/load/1")
            .setType("load")
            .build())
        .setJobState(JobState.VERIFYING, false);
    scheduler
        .getJobs()
        .get(JobDescription
            .newBuilder()
            .setPath("/load/2")
            .setType("load")
            .build())
        .setJobState(JobState.FAILED, false);
    scheduler
        .getJobs()
        .get(JobDescription
            .newBuilder()
            .setPath("/load/3")
            .setType("load")
            .build())
        .setJobState(JobState.SUCCEEDED, false);
    scheduler
        .getJobs()
        .get(JobDescription
            .newBuilder()
            .setPath("/load/4")
            .setType("load")
            .build())
        .setJobState(JobState.STOPPED, false);
    scheduler.cleanupStaleJob();
    assertEquals(2, scheduler
        .getJobs().size());
    assertTrue(scheduler
        .getJobs().containsKey(JobDescription
            .newBuilder()
            .setPath("/load/0")
            .setType("load")
            .build()));
    assertTrue(scheduler
        .getJobs().containsKey(JobDescription
            .newBuilder()
            .setPath("/load/1")
            .setType("load")
            .build()));
    IntStream.range(2, 5).forEach(
        i -> assertFalse(scheduler
            .getJobs().containsKey(JobDescription
            .newBuilder()
            .setPath("/load/" + i)
            .setType("load")
            .build())));
    Configuration.modifiableGlobal().unset(PropertyKey.JOB_RETENTION_TIME);
  }

  // test scheduler start and stop and start again with job meta store change
  @Test
  public void testStopScheduler() {
    String path = "/path/to/load";
    DefaultFileSystemMaster fsMaster = mock(DefaultFileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    DefaultWorkerProvider workerProvider =
        new DefaultWorkerProvider(fsMaster, fileSystemContext);
    InMemoryJobMetaStore metaStore = new InMemoryJobMetaStore();
    Scheduler scheduler = new Scheduler(fileSystemContext, workerProvider, metaStore);
    DoraLoadJob job =
        new DoraLoadJob(path, Optional.of("user"), "5", OptionalLong.of(100),
            false, true, false);
    scheduler.start();
    scheduler.submitJob(job);
    assertEquals(1, scheduler.getJobs().size());
    scheduler.stop();
    assertEquals(0, scheduler.getJobs().size());
    assertEquals(1, metaStore.getJobs().size());
    DoraLoadJob job2 =
        new DoraLoadJob("new", Optional.of("user"), "6", OptionalLong.of(100),
            false, true, false);
    metaStore.updateJob(job2);
    assertEquals(0, scheduler.getJobs().size());
    assertEquals(2, metaStore.getJobs().size());
    scheduler.start();
    assertEquals(2, scheduler.getJobs().size());
  }

  private class InMemoryJobMetaStore implements JobMetaStore {
    private final Map<String, alluxio.scheduler.job.Job<?>> mExistingJobs = new ConcurrentHashMap();

    @Override
    public void updateJob(alluxio.scheduler.job.Job<?> job) {
      mExistingJobs.put(job.getJobId(), job);
    }

    @Override
    public Set<alluxio.scheduler.job.Job<?>> getJobs() {
      return mExistingJobs.values().stream().collect(Collectors.toSet());
    }

    public alluxio.scheduler.job.Job<?> get(String jobId) {
      return mExistingJobs.get(jobId);
    }
  }
}
