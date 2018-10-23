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

package alluxio.security.group;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import alluxio.security.group.provider.IdentityUserGroupsMapping;

import org.junit.Test;

/**
 * Unit test for {@link alluxio.security.group.provider.IdentityUserGroupsMapping}.
 */
public final class IdentityUserGroupsMappingTest {

  /**
   * Tests the {@link IdentityUserGroupsMapping#getGroups(String)} method.
   */
  @Test
  public void userGroup() throws Throwable {
    String userName = "alluxio-user1";

    GroupMappingService groups = new IdentityUserGroupsMapping();

    assertNotNull(groups);
    assertNotNull(groups.getGroups(userName));
    assertEquals(groups.getGroups(userName).size(), 1);
    assertEquals(groups.getGroups(userName).get(0), userName);
  }
}
