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

package alluxio.job.plan.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.conf.ServerConfiguration;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.stress.job.IOConfig;
import alluxio.underfs.UfsManager;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, JobServerContext.class, FileSystemContext.class,
        AlluxioBlockStore.class})
public class IODefinitionTest {
  private static final List<WorkerInfo> JOB_WORKERS = new ImmutableList.Builder<WorkerInfo>()
          .add(new WorkerInfo().setId(0).setAddress(new WorkerNetAddress().setHost("host0")))
          .add(new WorkerInfo().setId(1).setAddress(new WorkerNetAddress().setHost("host1")))
          .add(new WorkerInfo().setId(2).setAddress(new WorkerNetAddress().setHost("host2")))
          .add(new WorkerInfo().setId(3).setAddress(new WorkerNetAddress().setHost("host3")))
          .build();

  private JobServerContext mJobServerContext;
  private FileSystem mMockFileSystem;
  private AlluxioBlockStore mMockBlockStore;
  private FileSystemContext mMockFsContext;

  @Before
  public void before() {
    mMockFileSystem = PowerMockito.mock(FileSystem.class);
    mMockBlockStore = PowerMockito.mock(AlluxioBlockStore.class);
    mMockFsContext = PowerMockito.mock(FileSystemContext.class);
    PowerMockito.mockStatic(AlluxioBlockStore.class);
    PowerMockito.when(AlluxioBlockStore.create(any(FileSystemContext.class)))
            .thenReturn(mMockBlockStore);
    PowerMockito.when(mMockFsContext.getClientContext())
            .thenReturn(ClientContext.create(ServerConfiguration.global()));
    PowerMockito.when(mMockFsContext.getClusterConf()).thenReturn(ServerConfiguration.global());
    PowerMockito.when(mMockFsContext.getPathConf(any(AlluxioURI.class)))
            .thenReturn(ServerConfiguration.global());
    mJobServerContext = new JobServerContext(mMockFileSystem, mMockFsContext,
            Mockito.mock(UfsManager.class));
  }

  @Test
  public void selectExecutors() throws Exception {
    verifySelection(1);
    verifySelection(2);
    verifySelection(4);
    verifySelection(5);
  }

  private void verifySelection(int workerNum) throws Exception {
    Set<WorkerInfo> workerSet = new HashSet<>(JOB_WORKERS);
    IOConfig jobConfig = new IOConfig(IOConfig.class.getCanonicalName(),
            ImmutableList.of(),
            16,
            1024,
            workerNum,
            "hdfs://namenode:9000/alluxio");
    Set<Pair<WorkerInfo, ArrayList<String>>> selection =
            new IODefinition().selectExecutors(jobConfig,
                    new ArrayList<>(JOB_WORKERS),
                    new SelectExecutorsContext(1, mJobServerContext));
    int expectedNum = Math.min(JOB_WORKERS.size(), workerNum);
    assertEquals(selection.size(), expectedNum);
    for (Pair<WorkerInfo, ArrayList<String>> p : selection) {
      WorkerInfo w = p.getFirst();
      assertTrue(workerSet.contains(w));
    }
  }
}
