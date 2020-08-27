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

package alluxio.client.file;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;

import alluxio.AlluxioURI;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.security.authorization.AclEntry;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link DelegatingFileSystem}.
 */
public class DelegatingFileSystemTest {
  private FileSystem mMockFileSystem;

  @Before
  public void before() throws Exception {
    mMockFileSystem = mock(FileSystem.class);
  }

  @Test
  public void mount() throws Exception {
    FileSystem fileSystem = new DelegatingFileSystem(mMockFileSystem);
    AlluxioURI alluxioPath = new AlluxioURI("/t");
    AlluxioURI ufsPath = new AlluxioURI("/u");
    MountPOptions mountOptions = MountPOptions
        .newBuilder()
        .setReadOnly(false)
        .setShared(false)
        .build();
    fileSystem.mount(alluxioPath, ufsPath, mountOptions);
    Mockito.verify(mMockFileSystem, atLeastOnce())
        .mount(eq(alluxioPath), eq(ufsPath), eq(mountOptions));
  }

  @Test
  public void setAcl() throws Exception {
    FileSystem fileSystem = new DelegatingFileSystem(mMockFileSystem);
    AlluxioURI alluxioPath = new AlluxioURI("/t");
    List<AclEntry> entries = Arrays.asList(AclEntry.fromCliString("user:nameduser:rwx"));
    SetAclPOptions setAclPOptions = SetAclPOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(5L).build())
        .setRecursive(true)
        .build();

    fileSystem.setAcl(alluxioPath, SetAclAction.MODIFY, entries, setAclPOptions);
    Mockito.verify(mMockFileSystem, atLeastOnce())
        .setAcl(eq(alluxioPath), eq(SetAclAction.MODIFY), eq(entries), eq(setAclPOptions));
  }
}
