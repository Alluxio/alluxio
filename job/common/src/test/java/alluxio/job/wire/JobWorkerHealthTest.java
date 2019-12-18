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

package alluxio.job.wire;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import org.junit.Test;

public class JobWorkerHealthTest {

  @Test
  public void testToProto() {
    JobWorkerHealth host = new JobWorkerHealth(1L,
        Lists.newArrayList(0.0, 0.0, 0.1),
        10,
        1,
        1,
        "host");

    JobWorkerHealth other = new JobWorkerHealth(host.toProto());

    assertEquals(host, other);
  }
}
