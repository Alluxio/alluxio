/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security.group;

import org.junit.Assert;
import org.junit.Test;

import tachyon.security.group.provider.IdentityUserGroupsMapping;

/**
 * Unit test for {@link tachyon.security.group.provider.IdentityUserGroupsMapping}.
 */
public final class IdentityUserGroupsMappingTest {

  /**
   * Tests the {@link IdentityUserGroupsMapping#getGroups(String)} method.
   *
   * @throws Throwable when the retrieval of the groups fails
   */
  @Test
  public void userGroupTest() throws Throwable {
    String userName = "tachyon-user1";

    GroupMappingService groups = new IdentityUserGroupsMapping();

    Assert.assertNotNull(groups);
    Assert.assertNotNull(groups.getGroups(userName));
    Assert.assertEquals(groups.getGroups(userName).size(), 1);
    Assert.assertEquals(groups.getGroups(userName).get(0), userName);
  }
}
