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

package alluxio.job.cmd.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests {@link LoadCliConfig}.
 */
public class LoadCliConfigTest {
  @Test
  public void jsonTest() throws Exception {
    LoadCliConfig config = new LoadCliConfig("/path/to/load", 3, 1, Collections.EMPTY_SET,
            Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, true);
    ObjectMapper mapper = new ObjectMapper();
    LoadCliConfig other = mapper.readValue(mapper.writeValueAsString(config), LoadCliConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void nullTest() {
    try {
      new LoadCliConfig(null, null, null, Collections.EMPTY_SET, Collections.EMPTY_SET,
              Collections.EMPTY_SET, Collections.EMPTY_SET, true);
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("The file path cannot be null", exception.getMessage());
    }
  }

  public void checkEquality(LoadCliConfig a, LoadCliConfig b) {
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a.getReplication(), b.getReplication());
    Assert.assertEquals(a.getJobSource(), b.getJobSource());
    Assert.assertEquals(a.getOperationType(), b.getOperationType());
    Assert.assertEquals(a, b);
  }
}
