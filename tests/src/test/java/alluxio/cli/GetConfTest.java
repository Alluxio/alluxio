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

package alluxio.cli;

import static org.junit.Assert.assertEquals;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.SystemOutRule;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

/**
 * Tests for {@link GetConf}.
 */
// TODO(binfan): create a SystemOutRule for unit test
public final class GetConfTest {
  private ByteArrayOutputStream mOutputStream = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mOutputStreamRule = new SystemOutRule(mOutputStream);

  @Test
  public void getConf() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2048");
    assertEquals(0, GetConf.getConf(PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2MB");
    assertEquals(0, GetConf.getConf(PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2MB\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "Nonsense");
    assertEquals(0, GetConf.getConf(PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("Nonsense\n", mOutputStream.toString());
  }

  @Test
  public void getConfWithCorrectUnit() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2048");
    assertEquals(0, GetConf.getConf("--unit", "B", PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2048");
    assertEquals(0, GetConf.getConf("--unit", "KB", PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2MB");
    assertEquals(0, GetConf.getConf("--unit", "KB", PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2MB");
    assertEquals(0, GetConf.getConf("--unit", "MB", PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2\n", mOutputStream.toString());
  }

  @Test
  public void getConfWithWrongUnit() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2048");
    assertEquals(1,
        GetConf.getConf("--unit", "bad_unit", PropertyKey.WORKER_MEMORY_SIZE.toString()));
  }
}
