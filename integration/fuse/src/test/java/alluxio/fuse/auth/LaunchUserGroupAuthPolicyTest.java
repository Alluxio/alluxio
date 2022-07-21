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
import alluxio.exception.FileDoesNotExistException;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
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
 * Unit tests for {@link LaunchUserGroupAuthPolicy}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AlluxioFuseUtils.class)
public class LaunchUserGroupAuthPolicyTest {
  private UserGroupFileSystem mFileSystem;
  private LaunchUserGroupAuthPolicy mAuthPolicy;

  @Before
  public void before() throws Exception {
    mFileSystem = new UserGroupFileSystem();
    mAuthPolicy = LaunchUserGroupAuthPolicy.create(mFileSystem,
        AlluxioFuseFileSystemOpts.create(mFileSystem.getConf()), Optional.empty());
    mAuthPolicy.init();
  }

  @Test
  public void setUserGroupIfNeeded() {
    AlluxioURI uri = new AlluxioURI("/TestSetUserGroupIfNeeded");
    mAuthPolicy.setUserGroupIfNeeded(uri);
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

  @Test
  public void setUserGroup() throws Exception {
    long uid = 123;
    long gid = 456;
    String userName = "myuser";
    String groupName = "mygroup";
    PowerMockito.mockStatic(AlluxioFuseUtils.class);
    PowerMockito.when(AlluxioFuseUtils.getUserName(eq(uid)))
        .thenReturn(Optional.of(userName));
    PowerMockito.when(AlluxioFuseUtils.getGroupName(eq(gid)))
        .thenReturn(Optional.of(groupName));
    AlluxioURI uri = new AlluxioURI("/TestSetUserGroup");
    mAuthPolicy.setUserGroup(uri, uid, gid);
    URIStatus status = mFileSystem.getStatus(uri);
    Assert.assertEquals(userName, status.getOwner());
    Assert.assertEquals(groupName, status.getGroup());
  }
}
