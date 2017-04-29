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

package alluxio.client.file.options;

import alluxio.CommonTestUtils;
import alluxio.ConfigurationRule;
import alluxio.LoginUserRule;
import alluxio.PropertyKey;
import alluxio.security.authorization.Mode;
import alluxio.security.group.provider.IdentityUserGroupsMapping;
import alluxio.thrift.CompleteUfsFileTOptions;
import alluxio.util.CommonUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Tests for the {@link CompleteUfsFileOptions} class.
 */
public final class CompleteUfsFileOptionsTest {
  private static final String TEST_USER = "test";

  @Rule
  public LoginUserRule mLoginUser = new LoginUserRule(TEST_USER);

  @Rule
  public ConfigurationRule mConfiguration = new ConfigurationRule(ImmutableMap
      .of(PropertyKey.SECURITY_GROUP_MAPPING_CLASS, IdentityUserGroupsMapping.class.getName()));

  /**
   * Tests that building a {@link CompleteUfsFileOptions} with the defaults works.
   */
  @Test
  public void defaults() throws IOException {
    CompleteUfsFileOptions options = CompleteUfsFileOptions.defaults();
    Assert.assertEquals(TEST_USER, options.getOwner());
    Assert.assertEquals(TEST_USER, options.getGroup());
    Assert.assertEquals(Mode.defaults().applyFileUMask(), options.getMode());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws IOException {
    Random random = new Random();
    String owner = CommonUtils.randomAlphaNumString(10);
    String group = CommonUtils.randomAlphaNumString(10);
    Mode mode = new Mode((short) random.nextInt());

    CompleteUfsFileOptions options = CompleteUfsFileOptions.defaults();
    options.setOwner(owner);
    options.setGroup(group);
    options.setMode(mode);

    Assert.assertEquals(owner, options.getOwner());
    Assert.assertEquals(group, options.getGroup());
    Assert.assertEquals(mode, options.getMode());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() throws IOException {
    Random random = new Random();
    String owner = CommonUtils.randomAlphaNumString(10);
    String group = CommonUtils.randomAlphaNumString(10);
    Mode mode = new Mode((short) random.nextInt());

    CompleteUfsFileOptions options = CompleteUfsFileOptions.defaults();
    options.setOwner(owner);
    options.setGroup(group);
    options.setMode(mode);

    CompleteUfsFileTOptions thriftOptions = options.toThrift();
    Assert.assertEquals(owner, thriftOptions.getOwner());
    Assert.assertEquals(group, thriftOptions.getGroup());
    Assert.assertEquals(mode.toShort(), thriftOptions.getMode());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(CompleteUfsFileOptions.class);
  }
}
