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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.eq;

import alluxio.security.group.provider.ShellBasedUnixGroupsMapping;
import alluxio.util.CommonUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for {@link alluxio.security.group.provider.ShellBasedUnixGroupsMapping}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CommonUtils.class)
public final class ShellBasedUnixGroupsMappingTest {

  private void setupShellMocks(String username, List<String> groups) throws IOException {
    PowerMockito.mockStatic(CommonUtils.class);
    PowerMockito.when(CommonUtils.getUnixGroups(eq(username))).thenReturn(groups);
  }

  /**
   * Tests the {@link ShellBasedUnixGroupsMapping#getGroups(String)} method.
   */
  @Test
  public void userGroup() throws Throwable {
    String userName = "alluxio-user1";
    String userGroup1 = "alluxio-user1-group1";
    String userGroup2 = "alluxio-user1-group2";
    List<String> userGroups = new ArrayList<>();
    userGroups.add(userGroup1);
    userGroups.add(userGroup2);
    setupShellMocks(userName, userGroups);

    GroupMappingService groups = new ShellBasedUnixGroupsMapping();

    assertNotNull(groups);
    assertNotNull(groups.getGroups(userName));
    assertEquals(groups.getGroups(userName).size(), 2);
  }
}
