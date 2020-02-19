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

import com.amazonaws.SdkClientException;
import com.amazonaws.util.EC2MetadataUtils;
import org.junit.Assert;
import org.junit.Before;
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
@PrepareForTest({EnvironmentUtils.class, EC2MetadataUtils.class})
public class UpdateCheckTest {

  @Before
  public void before() {
    PowerMockito.mockStatic(EnvironmentUtils.class);
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(false);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(false);
    Mockito.when(EnvironmentUtils.isGoogleComputeEngine()).thenReturn(false);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn("");
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(false);
    Mockito.when(EnvironmentUtils.isCFT(Mockito.anyString())).thenReturn(false);
    Mockito.when(EnvironmentUtils.isEMR(Mockito.anyString())).thenReturn(false);
    PowerMockito.mockStatic(EC2MetadataUtils.class);
  }

  @Test
  public void userAgentStringEmpty() throws Exception {
    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));

    System.out.println(userAgentString);
    Assert.assertTrue(
        userAgentString.equals(String.format("Alluxio/%s (cluster1)", ProjectConstants.VERSION)));
  }

  @Test
  public void userAgentStringDocker() throws Exception {
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(userAgentString
        .equals(String.format("Alluxio/%s (cluster1; docker)", ProjectConstants.VERSION)));
  }

  @Test
  public void userAgentStringK8s() throws Exception {
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(userAgentString.equals(
        String.format("Alluxio/%s (cluster1; docker; kubernetes)", ProjectConstants.VERSION)));
  }

  @Test
  public void userAgentStringGCP() throws Exception {
    Mockito.when(EnvironmentUtils.isGoogleComputeEngine()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(userAgentString.equals(
        String.format("Alluxio/%s (cluster1; gce)", ProjectConstants.VERSION)));
  }

  @Test
  public void userAgentStringEC2AMI() throws Exception {
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn("random123code");
    // When no user data in this ec2, null is returned
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn(null);

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(userAgentString.equals(
        String.format("Alluxio/%s (cluster1; ProductCode:random123code; ec2)",
            ProjectConstants.VERSION)));
  }

  @Test
  public void userAgentStringEC2CFT() throws Exception {
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn("random123code");
    Mockito.when(EnvironmentUtils.isCFT(Mockito.anyString())).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn("{ \"cft_configure\": {}}");

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(userAgentString.equals(
        String.format("Alluxio/%s (cluster1; ProductCode:random123code; cft; ec2)",
            ProjectConstants.VERSION)));
  }

  @Test
  public void userAgentStringEC2EMR() throws Exception {
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn("random123code");
    Mockito.when(EnvironmentUtils.isEMR(Mockito.anyString())).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn("emr_apps");

    String userAgentString = UpdateCheck.getUserAgentString("cluster1");
    Assert.assertTrue(userAgentString.equals(
        String.format("Alluxio/%s (cluster1; ProductCode:random123code; emr; ec2)",
            ProjectConstants.VERSION)));
  }
}
