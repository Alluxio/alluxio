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

package alluxio.job.plan.transform;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.job.JobServerContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.SelectExecutorsTest;
import alluxio.wire.WorkerInfo;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CompactDefinitionSelectExecutorsTest extends SelectExecutorsTest {

  private static final String INPUT_DIR = "/input";
  private static final String OUTPUT_DIR = "/output";

  @Test
  public void testExecutorsParallel() throws Exception {
    int tasksPerWorker = 10;
    int numCompactedFiles = 100;
    int totalFiles = 5000;

    CompactConfig config = new CompactConfig(null, INPUT_DIR, OUTPUT_DIR, "test",
        numCompactedFiles);

    List<URIStatus> inputFiles = new ArrayList<>();
    for (int i = 0; i < totalFiles; i++) {
      inputFiles.add(newFile(Integer.toString(i)));
    }

    when(mMockFileSystem.listStatus(new AlluxioURI(INPUT_DIR))).thenReturn(inputFiles);

    Set<Pair<WorkerInfo, ArrayList<CompactTask>>> result = new CompactDefinition().selectExecutors(
        config, SelectExecutorsTest.JOB_WORKERS, new SelectExecutorsContext(1,
            new JobServerContext(mMockFileSystem, mMockFileSystemContext, mMockUfsManager)));
    assertEquals(JOB_WORKERS.size() * tasksPerWorker, result.size());

    int allCompactTasks = 0;
    for (Pair<WorkerInfo, ArrayList<CompactTask>> tasks : result) {
      allCompactTasks += tasks.getSecond().size();
    }
    assertEquals(numCompactedFiles, allCompactTasks);
  }

  private URIStatus newFile(String name) {
    URIStatus mockFileStatus = Mockito.mock(URIStatus.class);
    when(mockFileStatus.isFolder()).thenReturn(false);
    when(mockFileStatus.getName()).thenReturn(name);
    return mockFileStatus;
  }
}
