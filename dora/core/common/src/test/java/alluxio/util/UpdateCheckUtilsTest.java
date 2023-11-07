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

package alluxio.util;

import alluxio.ProjectConstants;

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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unit tests for {@link UpdateCheckUtils}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({EnvironmentUtils.class, EC2MetadataUtils.class})
public class UpdateCheckUtilsTest {

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
  public void userAgentEnvironmentStringEmpty() {
    List<String> info = new ArrayList<>();
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));
    UpdateCheckUtils.addUserAgentEnvironments(info);
    Assert.assertEquals(1, info.size());
    Assert.assertEquals(String.format(UpdateCheckUtils.OS_FORMAT, OSUtils.OS_NAME),
        info.get(0));
  }

  @Test
  public void userAgentEnvironmentStringDocker() {
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));
    List<String> info = new ArrayList<>();
    UpdateCheckUtils.addUserAgentEnvironments(info);
    Assert.assertEquals(2, info.size());
    Assert.assertEquals(String.format(UpdateCheckUtils.OS_FORMAT, OSUtils.OS_NAME),
        info.get(0));
    Assert.assertEquals(UpdateCheckUtils.DOCKER_KEY, info.get(1));
  }

  @Test
  public void userAgentEnvironmentStringK8s() {
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));
    List<String> info = new ArrayList<>();
    UpdateCheckUtils.addUserAgentEnvironments(info);
    Assert.assertEquals(3, info.size());
    Assert.assertEquals(String.format(UpdateCheckUtils.OS_FORMAT, OSUtils.OS_NAME),
        info.get(0));
    Assert.assertEquals(UpdateCheckUtils.DOCKER_KEY, info.get(1));
    Assert.assertEquals(UpdateCheckUtils.KUBERNETES_KEY, info.get(2));
  }

  @Test
  public void userAgentEnvironmentStringGCP() {
    Mockito.when(EnvironmentUtils.isGoogleComputeEngine()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));
    List<String> info = new ArrayList<>();
    UpdateCheckUtils.addUserAgentEnvironments(info);
    Assert.assertEquals(2, info.size());
    Assert.assertEquals(String.format(UpdateCheckUtils.OS_FORMAT, OSUtils.OS_NAME),
        info.get(0));
    Assert.assertEquals(UpdateCheckUtils.GCE_KEY, info.get(1));
  }

  @Test
  public void userAgentEnvironmentStringEC2AMI() {
    String randomProductCode = "random123code";
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn(randomProductCode);
    // When no user data in this ec2, null is returned
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn(null);
    List<String> info = new ArrayList<>();
    UpdateCheckUtils.addUserAgentEnvironments(info);
    Assert.assertEquals(3, info.size());
    Assert.assertEquals(String.format(UpdateCheckUtils.OS_FORMAT, OSUtils.OS_NAME),
        info.get(0));
    Assert.assertEquals(String.format(UpdateCheckUtils.PRODUCT_CODE_FORMAT, randomProductCode),
        info.get(1));
    Assert.assertEquals(UpdateCheckUtils.EC2_KEY, info.get(2));
  }

  @Test
  public void userAgentEnvironmentStringEC2CFT() {
    String randomProductCode = "random123code";
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn(randomProductCode);
    Mockito.when(EnvironmentUtils.isCFT(Mockito.anyString())).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn("{ \"cft_configure\": {}}");

    List<String> info = new ArrayList<>();
    UpdateCheckUtils.addUserAgentEnvironments(info);
    Assert.assertEquals(4, info.size());
    Assert.assertEquals(String.format(UpdateCheckUtils.PRODUCT_CODE_FORMAT, randomProductCode),
        info.get(1));
    Assert.assertEquals(UpdateCheckUtils.CFT_KEY, info.get(2));
    Assert.assertEquals(UpdateCheckUtils.EC2_KEY, info.get(3));
  }

  @Test
  public void userAgentEnvironmentStringEC2EMR() {
    String randomProductCode = "random123code";
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn(randomProductCode);
    Mockito.when(EnvironmentUtils.isEMR(Mockito.anyString())).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn("emr_apps");
    List<String> info = new ArrayList<>();
    UpdateCheckUtils.addUserAgentEnvironments(info);
    Assert.assertEquals(4, info.size());
    Assert.assertEquals(String.format(UpdateCheckUtils.PRODUCT_CODE_FORMAT, randomProductCode),
        info.get(1));
    Assert.assertEquals(UpdateCheckUtils.EMR_KEY, info.get(2));
    Assert.assertEquals(UpdateCheckUtils.EC2_KEY, info.get(3));
  }

  @Test
  public void userAgent() {
    String userAgentString = UpdateCheckUtils.getUserAgentString("cluster1",
        CommonUtils.ProcessType.MASTER, new ArrayList<>());
    Pattern pattern = Pattern.compile(
        String.format("Alluxio\\/%s \\(cluster1(?:.+)[^;]\\)", ProjectConstants.VERSION));
    Matcher matcher = pattern.matcher(userAgentString);
    Assert.assertTrue(matcher.matches());
  }
}
