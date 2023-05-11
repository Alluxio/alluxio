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

package alluxio.job.cmd.migrate;

import alluxio.client.WriteType;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link MigrateCliConfig}.
 */
public class MigrateCliConfigTest {
  @Test
  public void jsonTest() throws Exception {
    MigrateCliConfig config = new MigrateCliConfig("/path/from",
            "/path/to", WriteType.THROUGH, true, 2);
    ObjectMapper mapper = new ObjectMapper();
    MigrateCliConfig other = mapper.readValue(
            mapper.writeValueAsString(config), MigrateCliConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void nullTest() {
    try {
      new MigrateCliConfig(null, "dst", WriteType.THROUGH, true, 1);
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("source must be set", exception.getMessage());
    }

    try {
      new MigrateCliConfig("src", null, WriteType.THROUGH, true, 1);
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("destination must be set", exception.getMessage());
    }
  }

  public void checkEquality(MigrateCliConfig a, MigrateCliConfig b) {
    Assert.assertEquals(a.getSource(), b.getSource());
    Assert.assertEquals(a.getDestination(), b.getDestination());
    Assert.assertEquals(a.getOperationType(), b.getOperationType());
    Assert.assertEquals(a.getJobSource(), b.getJobSource());
    Assert.assertEquals(a, b);
  }
}
