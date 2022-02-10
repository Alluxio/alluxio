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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.stress.cli.client.StressClientIOBench;
import alluxio.stress.client.ClientIOTaskResult;
import alluxio.util.JsonSerializable;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link StressClientIOBench}.
 */
public class StressClientIOBenchIntegrationTest extends AbstractStressBenchIntegrationTest {
  @Test
  public void readIO() throws Exception {
    // All the reads are in the same test, to re-use write results.
    // Only in-process will work for unit testing.

    String output1 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--start-ms", Long.toString(System.currentTimeMillis() + 5000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "Write",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output2 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--tag", "ReadArray-NOT_RANDOM",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "ReadArray",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output3 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "ReadArray-RANDOM",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "ReadArray",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output4 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "ReadByteBuffer",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "ReadByteBuffer",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output5 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "ReadFully",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "ReadFully",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output6 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "PosRead-test",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "PosRead",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });

    String output7 = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--read-random",
        "--tag", "PosReadFully",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "PosReadFully",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
    });
    generateAndVerifyReport(Arrays.asList(
        "Write", "ReadArray-NOT_RANDOM", "ReadArray-RANDOM", "ReadByteBuffer", "ReadFully",
        "PosRead-test", "PosReadFully"),
        output1, output2, output3, output4, output5, output6, output7);
  }

  @Test
  public void WriteTypeParameterTest() throws Exception {
    String[] writeType = new String[] {"MUST_CACHE", "CACHE_THROUGH", "THROUGH", "ASYNC_THROUGH"};

    for (int i = 0; i < writeType.length; i++) {
      validateTheResultWithWriteType(writeType[i]);
    }
  }

  private void validateTheResultWithWriteType(String writeType) throws Exception {
    String output = new StressClientIOBench().run(new String[] {
        "--in-process",
        "--start-ms", Long.toString(System.currentTimeMillis() + 5000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "Write",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
        "--write-type", writeType,
    });

    // convert the result into summary, and check whether it have errors.
    ClientIOTaskResult summary = (ClientIOTaskResult) JsonSerializable.fromJson(output);

    assertFalse(summary.getThreadCountResults().isEmpty());
    for (ClientIOTaskResult.ThreadCountResult threadResult :
        summary.getThreadCountResults().values()) {
      assertTrue(threadResult.getErrors().isEmpty());
    }
  }

  @Test
  public void writeTypeParameterALLTest() throws Exception {
    String[] input = new String[] {
        "--in-process",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/client/",
        "--operation", "Write",
        "--threads", "2",
        "--file-size", "1m",
        "--buffer-size", "128k",
        "--warmup", "0s", "--duration", "1s",
        "--write-type", "ALL",
    };

    StressClientIOBench spyBench = spy(StressClientIOBench.class);
    spyBench.run(input);
    ArgumentCaptor<String[]> captor = ArgumentCaptor.forClass(String[].class);

    // get the input of the run method, which is the executed task
    verify(spyBench, times(5)).run(captor.capture());
    List<String[]> capturedArgs = captor.getAllValues();

    // check the input task is the same as the expected batch task
    String[] possibleWriteType = {"ALL", "CACHE_THROUGH", "THROUGH",
        "MUST_CACHE", "ASYNC_THROUGH"};
    for (int i = 0; i < capturedArgs.size(); i++) {
      input[input.length - 1] = possibleWriteType[i];
      String[] executedTask = capturedArgs.get(i);

      assertTrue(compareString(input, executedTask));
    }
  }

  private boolean compareString(String[] a, String[] b) {
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; i++) {
      if (!a.equals(b)) {
        return false;
      }
    }
    return true;
  }
}
