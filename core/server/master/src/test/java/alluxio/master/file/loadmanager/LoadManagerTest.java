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

import static alluxio.master.file.loadmanager.LoadTestUtils.fileWithBlockLocations;
import static alluxio.master.file.loadmanager.LoadTestUtils.generateRandomBlockStatus;
import static alluxio.master.file.loadmanager.LoadTestUtils.generateRandomFileInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundRuntimeException;
import alluxio.exception.status.ResourceExhaustedRuntimeException;
import alluxio.exception.status.UnauthenticatedRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.master.file.FileSystemMaster;
import alluxio.resource.CloseableResource;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public final class LoadManagerTest {
  @Test
  public void testGetActiveWorkers() throws IOException {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    CloseableResource<BlockWorkerClient> blockWorkerClient = mock(CloseableResource.class);
    LoadManager loadManager = new LoadManager(fileSystemMaster, fileSystemContext);
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
    assertEquals(0, loadManager.getActiveWorkers().size());
    loadManager.updateWorkers();
    assertEquals(2, loadManager.getActiveWorkers().size());
    loadManager.updateWorkers();
    assertEquals(2, loadManager.getActiveWorkers().size());
    loadManager.updateWorkers();
    assertEquals(1, loadManager.getActiveWorkers().size());
    loadManager.updateWorkers();
    assertEquals(2, loadManager.getActiveWorkers().size());
  }

  @Test
  public void testSubmit() throws Exception {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    LoadManager loadManager = new LoadManager(fileSystemMaster, fileSystemContext);
    assertTrue(loadManager.submit("/path/to/load", 100, true));
    assertEquals(1, loadManager.getLoadJobs().size());
    assertEquals(100, loadManager.getLoadJobs().get("/path/to/load").getBandWidth());
    assertFalse(loadManager.submit("/path/to/load", 1000, false));
    assertEquals(1, loadManager.getLoadJobs().size());
    assertEquals(1000, loadManager.getLoadJobs().get("/path/to/load").getBandWidth());
    doThrow(new FileDoesNotExistException("test")).when(fileSystemMaster).checkAccess(any(), any());
    assertThrows(NotFoundRuntimeException.class,
        () -> loadManager.submit("/path/to/invalid", 1, true));
    doThrow(new InvalidPathException("test")).when(fileSystemMaster).checkAccess(any(), any());
    assertThrows(NotFoundRuntimeException.class,
        () -> loadManager.submit("/path/to/invalid", 1, true));
    doThrow(new AccessControlException("test")).when(fileSystemMaster).checkAccess(any(), any());
    assertThrows(UnauthenticatedRuntimeException.class,
        () -> loadManager.submit("/path/to/invalid", 1, true));
  }

  @Test
  public void testSubmitExceedsCapacity() {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
    LoadManager loadManager = new LoadManager(fileSystemMaster, fileSystemContext);
    IntStream.range(0, 100).forEach(
        i -> assertTrue(loadManager.submit(String.format("/path/to/load/%d", i), 1000, true)));
    assertThrows(
        ResourceExhaustedRuntimeException.class,
        () -> loadManager.submit("/path/to/load/101", 1000, true));
  }

  @Test
  public void testScheduling() throws Exception {
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    FileSystemContext fileSystemContext = mock(FileSystemContext.class);
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
    List<FileInfo> fileInfos = generateRandomFileInfo(100, 50, 64 * 1024 * 1024);
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

    LoadManager loadManager = new LoadManager(fileSystemMaster, fileSystemContext);
    LoadJob loadJob = new LoadJob("test", 1000, true);
    loadManager.submit(loadJob);
    loadManager.start();
    while (!loadManager.getLoadJobs().get("test").isDone()) {
      assertFalse(loadManager.submit(new LoadJob("test", 1000, true))
          && loadJob.getStatus() != LoadJob.LoadStatus.SUCCEEDED);
      Thread.sleep(1000);
    }
    Thread.sleep(1000);
    loadManager.stop();
    assertEquals(LoadJob.LoadStatus.SUCCEEDED, loadJob.getStatus());
    assertEquals(0, loadJob.getCurrentBlockCount());
    assertTrue(loadJob.getTotalBlockCount() > 5000);
    assertTrue(loadManager.submit(new LoadJob("test", 1000)));
  }
}
