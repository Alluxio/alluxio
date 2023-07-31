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

package alluxio.job.workflow.composite;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

import alluxio.job.JobConfig;
import alluxio.job.plan.persist.PersistConfig;
import alluxio.job.util.SerializationUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Tests {@link CompositeConfig}.
 */
public final class CompositeConfigTest {
  private static final CompositeConfig CONFIG;

  static {
    ArrayList<JobConfig> jobs = new ArrayList<>();
    jobs.add(new CompositeConfig(new ArrayList<>(), true));
    jobs.add(new CompositeConfig(new ArrayList<>(), false));
    jobs.add(new CompositeConfig(Lists.newArrayList(new PersistConfig("/", 1, true, "")), true));
    CONFIG = new CompositeConfig(jobs, true);
  }

  @Test
  public void jsonTest() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    CompositeConfig other = mapper.readValue(mapper.writeValueAsString(CONFIG),
        CompositeConfig.class);
    assertEquals(CONFIG, other);
  }

  @Test
  public void javaSerializationTest() throws Exception {
    byte[] bytes = SerializationUtils.serialize(CONFIG);
    assertNotNull(bytes);
    Serializable deserialized = SerializationUtils.deserialize(bytes);
    assertEquals(CompositeConfig.class, deserialized.getClass());
    assertEquals(CONFIG, deserialized);
  }
}
