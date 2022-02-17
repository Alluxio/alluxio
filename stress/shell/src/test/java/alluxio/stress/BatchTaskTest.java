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

package alluxio.stress;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;

import alluxio.stress.cli.StressMasterBench;
import alluxio.stress.cli.MasterBatchTask;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

public class BatchTaskTest {
  @Test
  public void ValidBatchTaskTest() throws Exception {
    String files = "1000";
    String threads = "10";
    String fileSize = "1k";
    String base = "alluxio:///";
    String[] input = new String[] {
        "MasterComprehensiveFileBatchTask",
        "--num-files", files,
        "--threads", threads,
        "--create-file-size", fileSize,
        "--base", base,
    };

    StressMasterBench masterBench = mock(StressMasterBench.class);
    new MasterBatchTask().run(input, masterBench);

    when(masterBench.run(any())).thenReturn("");
    // run the BatchTaskRunner and capture the input into StressBench
    ArgumentCaptor<String[]> captor = ArgumentCaptor.forClass(String[].class);
    verify(masterBench, times(7)).run(captor.capture());

    List<String[]> executedTasks = captor.getAllValues();

    String[] correctTask = new String[] {
        "--operation", "",
        "--base", base,
        "--threads", threads,
        "--stop-count", files,
        "--target-throughput", "1000000",
        "--warmup", "0s",
        "--create-file-size", fileSize,
        "--cluster",
    };
    String[] correctOperations = {"CreateFile", "ListDir", "ListDirLocated", "GetBlockLocations",
        "GetFileStatus", "OpenFile", "DeleteFile"};

    // verify that the expected task are executed
    for (int i = 0; i < correctOperations.length; i++) {
      // set the operation
      correctTask[1] = correctOperations[i];

      assertArrayEquals(correctTask, executedTasks.get(i));
    }
  }
}
