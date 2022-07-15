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
import static org.mockito.Mockito.when;

import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.Block;
import alluxio.master.file.FileSystemMaster;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class LoadJobTest {
  @Test
  public void testGetNextBatch()
      throws FileDoesNotExistException, AccessControlException, IOException, InvalidPathException {
    List<FileInfo> fileInfos = generateRandomFileInfo(5, 20, 64 * 1024 * 1024);
    FileSystemMaster fileSystemMaster = mock(FileSystemMaster.class);
    when(fileSystemMaster.listStatus(any(), any()))
        .thenReturn(fileInfos);
    LoadJob load = new LoadJob("test", 10000);
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
    LoadJob loadJob = new LoadJob("test", 10000);
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
}
