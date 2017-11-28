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

import alluxio.PropertyKey;
import alluxio.SystemOutRule;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

/**
 * Tests for {@link GetConfKey}.
 */
public final class GetConfKeyTest {
  private ByteArrayOutputStream mOutputStream = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mOutputStreamRule = new SystemOutRule(mOutputStream);

  @Test
  public void getConfKey() throws Exception {
    String varName = PropertyKey.WORKER_MEMORY_SIZE.getName().toUpperCase().replace(".", "_");
    assertEquals(0, GetConfKey.getConfKey(varName));
    assertEquals(String.format("%s\n", PropertyKey.WORKER_MEMORY_SIZE.getName()),
        mOutputStream.toString());

    mOutputStream.reset();
    varName = "AWS_ACCESSKEYID";
    assertEquals(0, GetConfKey.getConfKey(varName));
    assertEquals(String.format("%s\n", PropertyKey.S3A_ACCESS_KEY.getName()),
        mOutputStream.toString());

    mOutputStream.reset();
    assertEquals(1, GetConfKey.getConfKey("ALLUXIO_INVALID_COMMAND"));
  }
}
