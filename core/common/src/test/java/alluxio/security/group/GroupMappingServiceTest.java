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

import alluxio.ConfigurationRule;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.group.provider.IdentityUserGroupsMapping;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import java.io.Closeable;

/**
 * Unit test for {@link alluxio.security.group.GroupMappingService}.
 */
public final class GroupMappingServiceTest {

  /**
   * Tests the {@link GroupMappingService#getGroups(String)} method.
   */
  @Test
  public void group() throws Throwable {
    String userName = "alluxio-user1";
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    try (Closeable mConfigurationRule =
        new ConfigurationRule(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
            IdentityUserGroupsMapping.class.getName(), conf).toResource()) {
      GroupMappingService groups = GroupMappingService.Factory.get(conf);

      assertNotNull(groups);
      assertNotNull(groups.getGroups(userName));
      assertEquals(groups.getGroups(userName).size(), 1);
      assertEquals(groups.getGroups(userName).get(0), userName);
    }
  }
}
