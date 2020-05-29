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

package alluxio.stress.job;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class IOConfigTest {
  @Test
  public void json() throws Exception {
    IOConfig config = new IOConfig(IOConfig.class.getCanonicalName(),
            ImmutableList.of(),
            16,
            1024,
            1,
            "hdfs://namenode:9000/alluxio");
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(config);
    IOConfig other = mapper.readValue(json, IOConfig.class);
    assertTrue(config.equals(other));
  }
}
