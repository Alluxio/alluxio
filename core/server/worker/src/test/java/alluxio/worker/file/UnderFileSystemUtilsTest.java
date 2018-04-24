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

package alluxio.worker.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.wire.FileInfo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
* Tests {@link UnderFileSystemUtils}.
*/
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, UnderFileSystem.class})
public final class UnderFileSystemUtilsTest {

  private FileSystem mFileSystem;
  private UnderFileSystem mUnderFileSystem;

  // Test paths in Alluxio
  final AlluxioURI mAlluxioRoot = new AlluxioURI("/");
  final AlluxioURI mAlluxioA = new AlluxioURI("/a");
  final AlluxioURI mAlluxioB = new AlluxioURI("/a/b");
  final AlluxioURI mAlluxioFile = new AlluxioURI("/a/b/file");

  // Permissions for directories in Alluxio
  final String mOwnerRoot = "ownerRoot";
  final String mOwnerA = "ownerA";
  final String mOwnerB = "ownerB";
  final String mGroup = "group";
  final Mode mMode = Mode.createFullAccess();

  // Test paths in UFS
  final String mUfsRoot = "/ufs";
  final String mUfsA = "/ufs/a";
  final String mUfsB = "/ufs/a/b";
  final String mUfsFile = "/ufs/a/b/file";

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void before() throws Exception {
    mFileSystem = PowerMockito.mock(FileSystem.class);
    mUnderFileSystem = PowerMockito.mock(UnderFileSystem.class);

    Mockito.when(mUnderFileSystem.mkdirs(Mockito.anyString(), Mockito.any(MkdirsOptions.class)))
        .thenReturn(true);
  }

  /**
   * Tests the method {@link UnderFileSystemUtils#prepareFilePath} when root is mount point.
   */
  @Test
  public void prepareFilePath1() throws Exception {
    // Alluxio root is mount point
    Mockito.when(mFileSystem.getStatus(mAlluxioRoot))
        .thenReturn(createStatus(mOwnerRoot, mGroup, mMode, true));
    Mockito.when(mFileSystem.getStatus(mAlluxioA))
        .thenReturn(createStatus(mOwnerA, mGroup, mMode, false));
    Mockito.when(mFileSystem.getStatus(mAlluxioB))
        .thenReturn(createStatus(mOwnerB, mGroup, mMode, false));

    // Parent directories 'a' and 'b' do not exist in UFS
    Mockito.when(mUnderFileSystem.isDirectory(mUfsRoot)).thenReturn(true);
    Mockito.when(mUnderFileSystem.isDirectory(mUfsA)).thenReturn(false);
    Mockito.when(mUnderFileSystem.isDirectory(mUfsB)).thenReturn(false);

    UnderFileSystemUtils.prepareFilePath(mAlluxioFile, mUfsFile, mFileSystem, mUnderFileSystem);

    // Verify getStatus is called for all non-existent directories
    Mockito.verify(mFileSystem, Mockito.times(2)).getStatus(Mockito.any(AlluxioURI.class));
    Mockito.verify(mFileSystem, Mockito.times(1)).getStatus(mAlluxioA);
    Mockito.verify(mFileSystem, Mockito.times(1)).getStatus(mAlluxioA);
    // Verify mkdir is called with the correct permissions
    Mockito.verify(mUnderFileSystem, Mockito.times(1)).mkdirs(mUfsA, createMkdirsOptions(mOwnerA));
    Mockito.verify(mUnderFileSystem, Mockito.times(1)).mkdirs(mUfsB, createMkdirsOptions(mOwnerB));
  }

  /**
   * Tests the method {@link UnderFileSystemUtils#prepareFilePath} with nested mount points.
   */
  @Test
  public void prepareFilePath2() throws Exception {
    // Alluxio 'a' is mount point
    Mockito.when(mFileSystem.getStatus(mAlluxioRoot))
        .thenReturn(createStatus(mOwnerRoot, mGroup, mMode, true));
    Mockito.when(mFileSystem.getStatus(mAlluxioA))
        .thenReturn(createStatus(mOwnerA, mGroup, mMode, true));
    Mockito.when(mFileSystem.getStatus(mAlluxioB))
        .thenReturn(createStatus(mOwnerB, mGroup, mMode, false));

    // Parent directory 'b' does not exist in UFS
    Mockito.when(mUnderFileSystem.isDirectory(mUfsRoot)).thenReturn(true);
    Mockito.when(mUnderFileSystem.isDirectory(mUfsA)).thenReturn(true);
    Mockito.when(mUnderFileSystem.isDirectory(mUfsB)).thenReturn(false);

    UnderFileSystemUtils.prepareFilePath(mAlluxioFile, mUfsFile, mFileSystem, mUnderFileSystem);

    // Verify getStatus is called for all non-existent directories
    Mockito.verify(mFileSystem, Mockito.times(1)).getStatus(Mockito.any(AlluxioURI.class));
    Mockito.verify(mFileSystem, Mockito.times(1)).getStatus(mAlluxioB);

    // Verify mkdir is called with the correct permissions
    Mockito.verify(mUnderFileSystem, Mockito.times(1)).mkdirs(mUfsB, createMkdirsOptions(mOwnerB));
  }

  /**
   * Tests the method {@link UnderFileSystemUtils#prepareFilePath} with nested mount points when
   * the deepest parent mount point does not exist in UFS.
   */
  @Test
  public void prepareFilePath3() throws Exception {
    // An IOException should be thrown when mount point does not exist in UFS
    mThrown.expect(IOException.class);

    // Alluxio 'a' is mount point
    Mockito.when(mFileSystem.getStatus(mAlluxioRoot))
        .thenReturn(createStatus(mOwnerRoot, mGroup, mMode, true));
    Mockito.when(mFileSystem.getStatus(mAlluxioA))
        .thenReturn(createStatus(mOwnerA, mGroup, mMode, true));
    Mockito.when(mFileSystem.getStatus(mAlluxioB))
        .thenReturn(createStatus(mOwnerB, mGroup, mMode, false));

    // Parent directories 'a' and 'b' do not exist in UFS
    Mockito.when(mUnderFileSystem.isDirectory(mUfsRoot)).thenReturn(true);
    Mockito.when(mUnderFileSystem.isDirectory(mUfsA)).thenReturn(false);
    Mockito.when(mUnderFileSystem.isDirectory(mUfsB)).thenReturn(false);

    UnderFileSystemUtils.prepareFilePath(mAlluxioFile, mUfsFile, mFileSystem, mUnderFileSystem);

    // Verify getStatus is called for all non-existent directories
    Mockito.verify(mFileSystem, Mockito.times(1)).getStatus(Mockito.any(AlluxioURI.class));
    Mockito.verify(mFileSystem, Mockito.times(1)).getStatus(mAlluxioB);
    // Verify no directories are created in UFS
    Mockito.verify(mUnderFileSystem, Mockito.times(0)).mkdirs(Mockito.anyString(),
        Mockito.any(MkdirsOptions.class));
  }

  private MkdirsOptions createMkdirsOptions(String owner) {
    return MkdirsOptions.defaults().setCreateParent(false).setOwner(owner).setGroup(mGroup)
        .setMode(mMode);
  }

  private URIStatus createStatus(String owner, String group, Mode mode, boolean isMountPoint) {
    FileInfo info = new FileInfo();
    info.setMountPoint(true);
    info.setOwner(owner);
    info.setGroup(group);
    info.setMode(mode.toShort());
    info.setMountPoint(isMountPoint);
    return new URIStatus(info);
  }
}
