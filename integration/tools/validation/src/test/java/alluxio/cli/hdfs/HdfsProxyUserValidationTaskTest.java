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

package alluxio.cli.hdfs;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.cli.ValidationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthType;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class HdfsProxyUserValidationTaskTest {
  private static File sTestDir;
  private InstancedConfiguration mConf;

  @BeforeClass
  public static void prepareConfDir() throws IOException {
    sTestDir = ValidationTestUtils.prepareConfDir();
  }

  @Before
  public void prepareConf() {
    mConf = InstancedConfiguration.defaults();
    mConf.set(PropertyKey.CONF_DIR, sTestDir.getAbsolutePath());
  }

  public void prepareHdfsConfFiles(Map<String, String> coreSiteProps) {
    String coreSite = Paths.get(sTestDir.toPath().toString(), "core-site.xml").toString();
    ValidationTestUtils.writeXML(coreSite, coreSiteProps);

    String hdfsSite = Paths.get(sTestDir.toPath().toString(), "hdfs-site.xml").toString();
    ValidationTestUtils.writeXML(hdfsSite, ImmutableMap.of("key2", "value2"));
  }

  @Test
  public void skipped() {
    mConf.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL.getAuthName());
    HdfsProxyUserValidationTask task =
            new HdfsProxyUserValidationTask("hdfs://namenode:9000/alluxio", mConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.SKIPPED, result.getState());
  }

  @Test
  public void missingProxyUser() {
    String userName = System.getProperty("user.name");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("key1", "value1"));

    HdfsProxyUserValidationTask task =
            new HdfsProxyUserValidationTask("hdfs://namenode:9000/alluxio", mConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString(
            "Alluxio is not able to perform impersonation."));
    assertThat(result.getAdvice(), containsString(
            String.format("Please enable Alluxio user %s to impersonate", userName)));
  }

  @Test
  public void proxyUserNotWildcard() {
    String userName = System.getProperty("user.name");

    // Configured proxy users and groups, but not wildcard
    String proxyUserKey = String.format("hadoop.proxyuser.%s.users", userName);
    String proxyGroupKey = String.format("hadoop.proxyuser.%s.groups", userName);
    prepareHdfsConfFiles(ImmutableMap.of(proxyUserKey, "user1,user2", proxyGroupKey, "groups"));

    HdfsProxyUserValidationTask task =
            new HdfsProxyUserValidationTask("hdfs://namenode:9000/alluxio", mConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.WARNING, result.getState());
    assertThat(result.getResult(), containsString(
            String.format("%s=user1,user2 and %s=groups", proxyUserKey, proxyGroupKey)));
    assertThat(result.getAdvice(), containsString(
            "Please make sure that includes all users/groups Alluxio needs to impersonate as."));
  }

  @Test
  public void proxyUsersAndGroupsAllMissing() {
    String userName = System.getProperty("user.name");

    // Proxyuser configured for bob, not the running user
    prepareHdfsConfFiles(ImmutableMap.of("hadoop.proxyuser.bob.users", "user1,user3",
            "hadoop.proxyuser.bob.groups", "*"));

    HdfsProxyUserValidationTask task =
            new HdfsProxyUserValidationTask("hdfs://namenode:9000/alluxio", mConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString(
            "Alluxio is not able to perform impersonation."));
    assertThat(result.getAdvice(), containsString(
            String.format("Please enable Alluxio user %s to impersonate", userName)));
  }

  @Test
  public void wildcardProxyUsers() {
    String userName = System.getProperty("user.name");

    // Proxy users configured but not groups
    prepareHdfsConfFiles(ImmutableMap.of(
            String.format("hadoop.proxyuser.%s.users", userName), "*"));

    HdfsProxyUserValidationTask task =
            new HdfsProxyUserValidationTask("hdfs://namenode:9000/alluxio", mConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.OK, result.getState());
  }

  @Test
  public void wildcardProxyGroups() {
    String userName = System.getProperty("user.name");

    // Proxy groups configured but not users
    prepareHdfsConfFiles(ImmutableMap.of(
            String.format("hadoop.proxyuser.%s.groups", userName), "*"));

    HdfsProxyUserValidationTask task =
            new HdfsProxyUserValidationTask("hdfs://namenode:9000/alluxio", mConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.OK, result.getState());
  }
}
