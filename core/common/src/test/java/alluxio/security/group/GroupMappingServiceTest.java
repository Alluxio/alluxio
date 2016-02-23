/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.group;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.security.group.provider.IdentityUserGroupsMapping;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link alluxio.security.group.GroupMappingService}.
 */
public final class GroupMappingServiceTest {

  /**
   * Tests the {@link GroupMappingService#getGroups(String)} method.
   *
   * @throws Throwable when the retrieval of the groups fails
   */
  @Test
  public void groupTest() throws Throwable {
    String userName = "alluxio-user1";

    Configuration conf = new Configuration();
    conf.set(Constants.SECURITY_GROUP_MAPPING, IdentityUserGroupsMapping.class.getName());
    GroupMappingService groups = GroupMappingService.Factory.getUserToGroupsMappingService(conf);

    Assert.assertNotNull(groups);
    Assert.assertNotNull(groups.getGroups(userName));
    Assert.assertEquals(groups.getGroups(userName).size(), 1);
    Assert.assertEquals(groups.getGroups(userName).get(0), userName);
  }
}
