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

package alluxio.job.plan.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests {@link LoadConfig}.
 */
public final class LoadConfigTest {
  @Test
  public void jsonTest() throws Exception {
    LoadConfig config = new LoadConfig("/path/to/load", 3, Collections.EMPTY_SET,
        Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET);
    ObjectMapper mapper = new ObjectMapper();
    LoadConfig other = mapper.readValue(mapper.writeValueAsString(config), LoadConfig.class);
    checkEquality(config, other);
  }

  @Test
  public void nullTest() {
    try {
      new LoadConfig(null, null, Collections.EMPTY_SET,
          Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET);
      Assert.fail("Cannot create config with null path");
    } catch (NullPointerException exception) {
      Assert.assertEquals("The file path cannot be null", exception.getMessage());
    }
  }

  public void checkEquality(LoadConfig a, LoadConfig b) {
    Assert.assertEquals(a.getFilePath(), b.getFilePath());
    Assert.assertEquals(a.getReplication(), b.getReplication());
    Assert.assertEquals(a, b);
  }
}
