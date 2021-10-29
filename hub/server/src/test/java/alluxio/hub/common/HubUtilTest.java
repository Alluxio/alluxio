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

package alluxio.hub.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Unit tests for {@link HubUtil}.
 */
public final class HubUtilTest {
  public static final Logger LOG = LoggerFactory.getLogger(HubUtilTest.class);

  /**
   * Tests {@link HubUtil#getEnvVarsFromFile(String, org.slf4j.Logger)}.
   */
  @Test
  public void getEnvVarsFromFileSingle() {
    String content = "export ALLUXIO_VAR1=\"-DKEY=VALUE\"";
    Map<String, String> res = HubUtil.getEnvVarsFromFile(content, LOG);
    assertEquals(1, res.size());
    for (Map.Entry entry : res.entrySet()) {
      assertTrue(entry.getKey().equals("ALLUXIO_VAR1"));
      assertTrue(entry.getValue().equals("-DKEY=VALUE"));
    }
  }

  /**
   * Tests {@link HubUtil#getEnvVarsFromFile(String, org.slf4j.Logger)}.
   */
  @Test
  public void getEnvVarsFromFileMultipleProperties() {
    String content = "  export  ALLUXIO_VAR1  =  \" -DKEY1=VALUE1 -DKEY2=VALUE2 \"  ";
    Map<String, String> res = HubUtil.getEnvVarsFromFile(content, LOG);
    assertEquals(1, res.size());
    for (Map.Entry entry : res.entrySet()) {
      assertTrue(entry.getKey().equals("ALLUXIO_VAR1"));
      assertTrue(entry.getValue().equals(" -DKEY1=VALUE1 -DKEY2=VALUE2 "));
    }
  }

  /**
   * Tests {@link HubUtil#getEnvVarsFromFile(String, org.slf4j.Logger)}.
   */
  @Test
  public void getEnvVarsFromFileMultipleVars() {
    String content = "export ALLUXIO_VAR0=\" -DKEY1=VALUE1 -DKEY2=VALUE2 \"\n"
        + "export ALLUXIO_VAR1=\" -DKEY1=VALUE1 \"";
    Map<String, String> res = HubUtil.getEnvVarsFromFile(content, LOG);
    assertEquals(2, res.size());
    int i = 0;
    for (Map.Entry entry : res.entrySet()) {
      assertTrue(entry.getKey().equals("ALLUXIO_VAR" + i));
      if (i++ == 0) {
        assertTrue(entry.getValue().equals(" -DKEY1=VALUE1 -DKEY2=VALUE2 "));
        continue;
      }
      assertTrue(entry.getValue().equals(" -DKEY1=VALUE1 "));
    }
  }

  /**
   * Tests {@link HubUtil#getEnvVarsFromFile(String, org.slf4j.Logger)}.
   */
  @Test
  public void getEnvVarsFromFileInvalid() {
    String content = "expport ALLUXIO_VAR0=\" -DKEY1=VALUE1 -DKEY2=VALUE2 \"\n"
        + "ALLUXIO_VAR1=\" -DKEY1=VALUE1 \"";
    Map<String, String> res = HubUtil.getEnvVarsFromFile(content, LOG);
    assertEquals(0, res.size());
  }
}
