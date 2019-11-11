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

import org.junit.Test;

import java.io.IOException;

/**
 * Tests the wire format of {@link PlanInfo}.
 */
public final class PlanInfoTest {

  @Test
  public void testToProto() throws IOException {
    PlanInfo planInfo = new PlanInfo(1, "test", Status.COMPLETED, 10, null);
    PlanInfo otherPlanInfo = new PlanInfo(planInfo.toProto());

    assertEquals(planInfo, otherPlanInfo);
  }
}
