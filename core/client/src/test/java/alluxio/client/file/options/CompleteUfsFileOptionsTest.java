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

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.util.ClientTestUtils;
import alluxio.security.authorization.PermissionStatus;
import alluxio.security.group.provider.IdentityUserGroupsMapping;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for the {@link CompleteUfsFileOptions} class.
 */
public class CompleteUfsFileOptionsTest {
  /**
   * Tests that building an {@link CompleteUfsFileOptions} with the defaults works.
   */
  @Test
  public void defaultsTest() throws IOException {
    ClientContext.getConf().set(Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE");
    ClientContext.getConf().set(Constants.SECURITY_LOGIN_USERNAME, "foo");
    // Use IdentityUserGroupMapping to map user "foo" to group "foo".
    ClientContext.getConf().set(Constants.SECURITY_GROUP_MAPPING,
        IdentityUserGroupsMapping.class.getName());

    CompleteUfsFileOptions options = CompleteUfsFileOptions.defaults();

    PermissionStatus expectedPs =
        PermissionStatus.defaults().applyFileUMask(ClientContext.getConf());

    Assert.assertEquals("foo", options.getUser());
    Assert.assertEquals("foo", options.getGroup());
    Assert.assertEquals(expectedPs.getPermission().toShort(), options.getPermission());
    ClientTestUtils.resetClientContext();
  }
}
