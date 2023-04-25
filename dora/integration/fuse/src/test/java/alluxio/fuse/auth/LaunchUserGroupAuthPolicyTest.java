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

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.AlluxioFuseUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

/**
 * Unit tests for {@link LaunchUserGroupAuthPolicy}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AlluxioFuseUtils.class)
public class LaunchUserGroupAuthPolicyTest extends AbstractAuthPolicyTest {
  @Before
  public void before() throws Exception {
    mAuthPolicy = LaunchUserGroupAuthPolicy.create(mFileSystem,
        Configuration.global(), Optional.empty());
    mAuthPolicy.init();
  }

  @Test
  public void setUserGroupIfNeeded() {
    AlluxioURI uri = new AlluxioURI("/TestSetUserGroupIfNeeded");
    mAuthPolicy.setUserGroupIfNeeded(uri);
    // No need to set user group
    Assert.assertThrows(FileDoesNotExistException.class, () -> mFileSystem.getStatus(uri));
  }

  @Test
  public void getUserGroup() {
    Optional<Long> uid = mAuthPolicy.getUid("randomUser");
    Optional<Long> gid = mAuthPolicy.getGid("randomGroup");
    Assert.assertTrue(uid.isPresent());
    Assert.assertTrue(gid.isPresent());
    Assert.assertEquals(AlluxioFuseUtils.getSystemUid(), (long) uid.get());
    Assert.assertEquals(AlluxioFuseUtils.getSystemGid(), (long) gid.get());
  }
}
