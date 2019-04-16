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

package alluxio.underfs.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Mode;
import alluxio.security.group.provider.IdentityUserGroupsMapping;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.ModeUtils;

import com.google.common.testing.EqualsTester;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Tests for the {@link MkdirsOptions} class.
 */
public final class MkdirsOptionsTest {

  private InstancedConfiguration mConfiguration;

  @Before
  public void before() {
    mConfiguration = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  /**
   * Tests for default {@link MkdirsOptions}.
   */
  @Test
  public void defaults() throws IOException {
    MkdirsOptions options = MkdirsOptions.defaults(mConfiguration);

    // Verify the default createParent is true.
    assertTrue(options.getCreateParent());
    // Verify that the owner and group are not set.
    assertNull(options.getOwner());
    assertNull(options.getGroup());
    String umask = mConfiguration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK);
    assertEquals(ModeUtils.applyDirectoryUMask(Mode.defaults(), umask), options.getMode());
  }

  /**
   * Tests for building an {@link MkdirsOptions} with a security enabled
   * configuration.
   */
  @Test
  public void securityEnabled() throws IOException {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
    conf.set(PropertyKey.SECURITY_LOGIN_USERNAME, "foo");
    // Use IdentityUserGroupMapping to map user "foo" to group "foo".
    conf.set(PropertyKey.SECURITY_GROUP_MAPPING_CLASS,
        IdentityUserGroupsMapping.class.getName());

    MkdirsOptions options = MkdirsOptions.defaults(mConfiguration);

    // Verify the default createParent is true.
    assertTrue(options.getCreateParent());
    // Verify that the owner and group are not set.
    assertNull(options.getOwner());
    assertNull(options.getGroup());
    String umask = mConfiguration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK);
    assertEquals(ModeUtils.applyDirectoryUMask(Mode.defaults(), umask), options.getMode());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    boolean createParent = random.nextBoolean();
    String owner = CommonUtils.randomAlphaNumString(10);
    String group = CommonUtils.randomAlphaNumString(10);
    Mode mode = new Mode((short) random.nextInt());

    MkdirsOptions options = MkdirsOptions.defaults(mConfiguration);
    options.setCreateParent(createParent);
    options.setOwner(owner);
    options.setGroup(group);
    options.setMode(mode);

    assertEquals(createParent, options.getCreateParent());
    assertEquals(owner, options.getOwner());
    assertEquals(group, options.getGroup());
    assertEquals(mode, options.getMode());
  }

  @Test
  public void equalsTest() throws Exception {
    new EqualsTester()
        .addEqualityGroup(
            MkdirsOptions.defaults(mConfiguration),
            MkdirsOptions.defaults(mConfiguration))
        .testEquals();
  }
}
