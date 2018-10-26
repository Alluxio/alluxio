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

package alluxio.client.job;

import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link JobContext}.
 */
public final class JobContextTest {
  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.MASTER_HOSTNAME, "host1",
      PropertyKey.JOB_MASTER_HOSTNAME, "host2"
  ));

  @Test
  public void getAddress() throws Exception {
    try (JobContext context = JobContext.create()) {
      assertEquals("host2", context.getJobMasterAddress().getHostName());
    }
  }
}
