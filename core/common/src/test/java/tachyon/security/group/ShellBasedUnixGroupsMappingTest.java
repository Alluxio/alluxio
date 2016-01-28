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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import tachyon.security.group.provider.ShellBasedUnixGroupsMapping;
import tachyon.util.CommonUtils;

/**
 * Unit test for {@link tachyon.security.group.provider.ShellBasedUnixGroupsMapping}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CommonUtils.class)
public final class ShellBasedUnixGroupsMappingTest {

  private void setupShellMocks(String username, List<String> groups) throws IOException {
    PowerMockito.mockStatic(CommonUtils.class);
    PowerMockito.when(CommonUtils.getUnixGroups(Mockito.eq(username))).thenReturn(groups);
  }

  /**
   * Tests the {@link ShellBasedUnixGroupsMapping#getGroups(String)} method.
   *
   * @throws Throwable when retrieval of the groups fails
   */
  @Test
  public void userGroupTest() throws Throwable {
    String userName = "tachyon-user1";
    String userGroup1 = "tachyon-user1-group1";
    String userGroup2 = "tachyon-user1-group2";
    List<String> userGroups = new ArrayList<String>();
    userGroups.add(userGroup1);
    userGroups.add(userGroup2);
    setupShellMocks(userName,userGroups);

    GroupMappingService groups = new ShellBasedUnixGroupsMapping();

    Assert.assertNotNull(groups);
    Assert.assertNotNull(groups.getGroups(userName));
    Assert.assertEquals(groups.getGroups(userName).size(), 2);
  }
}
