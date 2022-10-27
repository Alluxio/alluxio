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

package alluxio.fuse.auth;

import static org.mockito.ArgumentMatchers.eq;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.fuse.AlluxioFuseUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

/**
 * Unit tests for {@link CustomAuthPolicy}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AlluxioFuseUtils.class)
public class CustomAuthPolicyTest extends AbstractAuthPolicyTest {
  private static final long UID = 123;
  private static final long GID = 456;
  private static final String USER = "customUser";
  private static final String GROUP = "customGroup";

  @Before
  public void before() throws Exception {
    InstancedConfiguration conf = Configuration.modifiableGlobal();
    conf.set(PropertyKey.FUSE_AUTH_POLICY_CLASS,
        "alluxio.fuse.auth.CustomAuthPolicy", Source.RUNTIME);
    conf.set(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_USER, USER, Source.RUNTIME);
    conf.set(PropertyKey.FUSE_AUTH_POLICY_CUSTOM_GROUP, GROUP, Source.RUNTIME);
    PowerMockito.mockStatic(AlluxioFuseUtils.class);
    PowerMockito.when(AlluxioFuseUtils.getUid(eq(USER)))
        .thenReturn(Optional.of(UID));
    PowerMockito.when(AlluxioFuseUtils.getGidFromGroupName(eq(GROUP)))
        .thenReturn(Optional.of(GID));
    mAuthPolicy = CustomAuthPolicy.create(mFileSystem, conf, Optional.empty());
    mAuthPolicy.init();
  }

  @Test
  public void setUserGroupIfNeed() throws Exception {
    AlluxioURI uri = new AlluxioURI("/SetUserGroupIfNeed");
    mAuthPolicy.setUserGroupIfNeeded(uri);
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(USER, status.getOwner());
    Assert.assertEquals(GROUP, status.getGroup());
  }

  @Test
  public void getUidGid() {
    Optional<Long> uid = mAuthPolicy.getUid("randomUser");
    Optional<Long> gid = mAuthPolicy.getGid("randomGroup");
    Assert.assertTrue(uid.isPresent());
    Assert.assertTrue(gid.isPresent());
    Assert.assertEquals(UID, (long) uid.get());
    Assert.assertEquals(GID, (long) gid.get());
  }
}
