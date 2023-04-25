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

package alluxio.client.cli.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for getting job status command.
 */
public final class StatCommandTest extends JobShellTest {
  @Test
  public void statTest() throws Exception {
    String pathStr = "/pathStr";

    long jobId = runPersistJob(pathStr);
    waitForJobToFinish(jobId);
    sJobShell.run("stat", "-v", Long.toString(jobId));

    String[] output = mOutput.toString().split("\n");

    assertEquals(String.format("ID: %s", jobId), output[0]);
    assertEquals(String.format("Name: Persist"), output[1]);
    assertTrue(output[2].contains("Description: PersistConfig"));
    assertTrue(output[2].contains(pathStr));
    assertEquals("Status: COMPLETED", output[3]);
    assertEquals("Task 0", output[4]);
    assertTrue(output[5].contains("\tWorker: "));
    assertEquals("\tStatus: COMPLETED", output[6]);
  }
}
