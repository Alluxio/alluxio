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
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.jnifuse.struct.FuseContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Unit tests for {@link SystemUserGroupAuthPolicy}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AlluxioFuseUtils.class)
public class SystemUserGroupAuthPolicyTest extends AbstractAuthPolicyTest {
  private static final long UID = 123;
  private static final long GID = 456;
  private static final String USER = "systemUser";
  private static final String GROUP = "systemGroup";

  @Before
  public void before() throws Exception {
    mAuthPolicy = SystemUserGroupAuthPolicy.create(mFileSystem,
        Configuration.global(), Optional.of(mFuseFileSystem));
    mAuthPolicy.init();
    PowerMockito.mockStatic(AlluxioFuseUtils.class);
    PowerMockito.when(AlluxioFuseUtils.getUserName(eq(UID)))
        .thenReturn(Optional.of(USER));
    PowerMockito.when(AlluxioFuseUtils.getGroupName(eq(GID)))
        .thenReturn(Optional.of(GROUP));
    PowerMockito.when(AlluxioFuseUtils.getUid(eq(USER)))
        .thenReturn(Optional.of(UID));
    PowerMockito.when(AlluxioFuseUtils.getGidFromGroupName(eq(GROUP)))
        .thenReturn(Optional.of(GID));
  }

  @Test
  public void setUserGroupIfNeed() throws Exception {
    AlluxioURI uri = new AlluxioURI("/SetUserGroupIfNeed");
    FuseContext context = FuseContext.of(ByteBuffer.allocate(32));
    context.uid.set(UID);
    context.gid.set(GID);
    mFuseFileSystem.setContext(context);
    mAuthPolicy.setUserGroupIfNeeded(uri);
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(USER, status.getOwner());
    Assert.assertEquals(GROUP, status.getGroup());
  }

  @Test
  public void getUidGid() {
    Optional<Long> uid = mAuthPolicy.getUid(USER);
    Optional<Long> gid = mAuthPolicy.getGid(GROUP);
    Assert.assertTrue(uid.isPresent());
    Assert.assertTrue(gid.isPresent());
    Assert.assertEquals(UID, (long) uid.get());
    Assert.assertEquals(GID, (long) gid.get());
  }
}
