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

package alluxio.job.plan.transform;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test for {@link HiveConstants}.
 */
public class HiveConstantsTest {

  @Test
  public void testGetHiveConstantType() {
    assertEquals(HiveConstants.Types.CHAR, HiveConstants.Types.getHiveConstantType("char(20)"));
    assertEquals(HiveConstants.Types.VARCHAR,
        HiveConstants.Types.getHiveConstantType("varchar(20)"));
    assertEquals(HiveConstants.Types.DECIMAL,
        HiveConstants.Types.getHiveConstantType("decimal(10, 20)"));
  }
}
