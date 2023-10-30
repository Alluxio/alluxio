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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.master.job.DoraLoadJob;
import alluxio.master.job.LoadDataSubTask;
import alluxio.master.job.LoadMetadataSubTask;
import alluxio.master.job.LoadSubTask;
import alluxio.master.job.UfsStatusIterable;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class DoraLoadJobTest {
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  private String mLocalUfsRoot;
  private UnderFileSystem mLocalUfs;
  private int mVirtualBlockSize;

  @Before
  public void before() throws IOException {
    mLocalUfsRoot = mTestFolder.getRoot().getAbsolutePath();
    mLocalUfs = UnderFileSystem.Factory.create(mLocalUfsRoot,
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    mVirtualBlockSize = 2 * Constants.MB;
    Configuration.set(PropertyKey.DORA_READ_VIRTUAL_BLOCK_SIZE, mVirtualBlockSize);
  }

  @Test
  public void testGetNextTaskWithVirtualBlocks() throws IOException {
    int testLength = 3 * Constants.MB;
    String testPath = createByteFileInUfs("testFileVirtualBlock", testLength);
    // test get next task is working properly with virtual blocks
    Iterator<UfsStatus> iterator = new UfsStatusIterable(mLocalUfs, testPath, Optional.empty(),
        Predicates.alwaysTrue()).iterator();
    DoraLoadJob loadJob =
        new DoraLoadJob(mLocalUfsRoot, Optional.of("user"), "1", OptionalLong.empty(), false, true,
            false, false, Optional.empty(), iterator, mLocalUfs, 1);
    Set<WorkerInfo> workers = ImmutableSet.of(
            new WorkerInfo().setId(1).setAddress(
                new WorkerNetAddress().setHost("worker1").setRpcPort(1234)));
    List<DoraLoadJob.DoraLoadTask> tasks = loadJob.getNextTasks(workers);
    List<LoadSubTask> subTasks = tasks.get(0).getSubTasks();
    assertEquals(3, subTasks.size());
    for (LoadSubTask subTask : subTasks) {
      if (subTask instanceof LoadMetadataSubTask) {
        assertEquals(subTask.getUfsPath(), testPath);
        assertEquals(subTask.asString(), testPath + "0");
      }
      else {
        LoadDataSubTask loadDataSubTask = (LoadDataSubTask) subTask;
        if (loadDataSubTask.getOffset() == 0) {
          assertEquals(loadDataSubTask.getUfsPath(), testPath);
          assertEquals(loadDataSubTask.getLength(), mVirtualBlockSize);
          assertEquals(loadDataSubTask.asString(), testPath + "0");
        }
        else if (loadDataSubTask.getOffset() == mVirtualBlockSize) {
          assertEquals(loadDataSubTask.getLength(), testLength - mVirtualBlockSize);
          assertEquals(loadDataSubTask.asString(), testPath + "1");
        }
      }
    }
  }

  @Test
  public void testGetNextTaskWithReplicas() throws IOException, InterruptedException {
    Thread.sleep(2000); // sleep so we can refresh hash ring
    int testLength = 3 * Constants.MB;
    String testPath = createByteFileInUfs("loadReplicas", testLength);
    // test get next task is working properly with virtual blocks
    UfsStatusIterable iterable =
        new UfsStatusIterable(mLocalUfs, testPath, Optional.empty(), Predicates.alwaysTrue());
    DoraLoadJob loadJob =
        new DoraLoadJob(testPath, Optional.of("user"), "1", OptionalLong.empty(), false, true,
            false, false, Optional.empty(), iterable.iterator(), mLocalUfs, 3);
    Set<WorkerInfo> workers = ImmutableSet.of(
            new WorkerInfo().setId(1).setAddress(
                new WorkerNetAddress().setHost("worker1").setRpcPort(1234)),
            new WorkerInfo().setId(2).setAddress(
                new WorkerNetAddress().setHost("worker2").setRpcPort(1234)),
        new WorkerInfo().setId(3).setAddress(
                new WorkerNetAddress().setHost("worker3").setRpcPort(1234)));
    List<DoraLoadJob.DoraLoadTask> tasks = loadJob.getNextTasks(workers);
    assertEquals(3, tasks.size());
    for (DoraLoadJob.DoraLoadTask task : tasks) {
      List<LoadSubTask> subTasks = task.getSubTasks();
      assertEquals(3, subTasks.size());
    }
    // test 2 replicas with 3 workers
    loadJob =
        new DoraLoadJob(mLocalUfsRoot, Optional.of("user"), "1", OptionalLong.empty(), false, true,
            false, false, Optional.empty(), iterable.iterator(), mLocalUfs, 2);
    tasks = loadJob.getNextTasks(workers);
    List<LoadSubTask> subTasks =
        tasks.stream().flatMap(task -> task.getSubTasks().stream()).collect(Collectors.toList());
    assertEquals(6, subTasks.size());
    // test 4 replicas with 3 workers
    assertThrows(ResourceExhaustedRuntimeException.class, () -> {
      new DoraLoadJob(mLocalUfsRoot, Optional.of("user"), "1", OptionalLong.empty(), false, true,
          false, false, Optional.empty(), iterable.iterator(), mLocalUfs, 4).getNextTasks(workers);
    });
  }

  protected String createByteFileInUfs(String fileName, int length) throws IOException {
    if (fileName.startsWith("/")) {
      fileName = fileName.substring(1);
    }
    File f = mTestFolder.newFile(fileName);
    Files.write(f.toPath(), BufferUtils.getIncreasingByteArray(length));
    return f.getAbsolutePath();
  }
}
