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

package alluxio.master.meta;

import alluxio.ProjectConstants;
import alluxio.util.EnvironmentUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for {@link UpdateCheck}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(EnvironmentUtils.class)
public class UpdateCheckTest {

  @Test
  public void userAgentStringEmpty() throws Exception {
    PowerMockito.mockStatic(EnvironmentUtils.class);
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(false);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(false);

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(
        userAgentString.equals(String.format("Alluxio/%s (cluster1)", ProjectConstants.VERSION)));
  }

  @Test
  public void userAgentStringDocker() throws Exception {
    PowerMockito.mockStatic(EnvironmentUtils.class);
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(false);

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(userAgentString
        .equals(String.format("Alluxio/%s (cluster1; docker)", ProjectConstants.VERSION)));
  }

  @Test
  public void userAgentStringK8s() throws Exception {
    PowerMockito.mockStatic(EnvironmentUtils.class);
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(true);

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(userAgentString.equals(
        String.format("Alluxio/%s (cluster1; docker; kubernetes)", ProjectConstants.VERSION)));
  }
}
