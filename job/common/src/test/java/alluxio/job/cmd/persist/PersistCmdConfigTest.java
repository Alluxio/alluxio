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

package alluxio.job.cmd.persist;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link PersistCmdConfig}.
 */
public class PersistCmdConfigTest {
  @Test
  public void jsonTest() throws Exception {
    PersistCmdConfig config = new PersistCmdConfig("path", 0, true, "ufs");
    ObjectMapper mapper = new ObjectMapper();
    PersistCmdConfig other = mapper.readValue(
            mapper.writeValueAsString(config), PersistCmdConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void nullTest() {
    try {
      new PersistCmdConfig(null, 0, true, "ufs");
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("The file path cannot be null", exception.getMessage());
    }

    try {
      new PersistCmdConfig("path", 0, true, null);
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("The UFS path cannot be null", exception.getMessage());
    }
  }

  public void checkEquality(PersistCmdConfig a, PersistCmdConfig b) {
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a.getUfsPath(), b.getUfsPath());
    Assert.assertEquals(a.getMountId(), b.getMountId());
    Assert.assertEquals(a.isOverwrite(), b.isOverwrite());
    Assert.assertEquals(a.getOperationType(), b.getOperationType());
    Assert.assertEquals(a.getJobSource(), b.getJobSource());
    Assert.assertEquals(a, b);
  }
}
