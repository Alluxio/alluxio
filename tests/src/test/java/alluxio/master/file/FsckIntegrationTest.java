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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.underfs.UnderFileSystem;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration test for the fsck command.
 */
public class FsckIntegrationTest {
  private static final AlluxioURI DIRECTORY = new AlluxioURI("/dir");
  private static final AlluxioURI FILE = new AlluxioURI("/dir/file");

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  private FileSystemMaster mFileSystemMaster;
  private FileSystem mFileSystem;

  @Before
  public final void before() throws Exception {
    mFileSystemMaster =
        mLocalAlluxioClusterResource.get().getMaster().getInternalMaster().getFileSystemMaster();
    mFileSystem = FileSystem.Factory.get();
    CreateDirectoryOptions dirOptions =
        CreateDirectoryOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
    CreateFileOptions fileOptions =
        CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
    mFileSystem.createDirectory(DIRECTORY, dirOptions);
    mFileSystem.createFile(FILE, fileOptions).close();
  }

  /**
   * Tests the {@link FileSystemMaster#fsck(alluxio.AlluxioURI)} method when all files are consistent.
   */
  @Test
  public void fsckConsistent() throws Exception {
    Assert.assertEquals(new ArrayList<AlluxioURI>(), mFileSystemMaster.fsck(new AlluxioURI("/")));
  }

  /**
   * Tests the {@link FileSystemMaster#fsck(AlluxioURI)} method when no files are consistent.
   */
  @Test
  public void fsckInconsistent() throws Exception {
    String ufsDirectory = mFileSystem.getStatus(DIRECTORY).getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.get(ufsDirectory);
    ufs.delete(ufsDirectory, true);
    List<AlluxioURI> expected = new ArrayList<>();
    expected.add(DIRECTORY);
    expected.add(FILE);
    Collections.sort(expected);
    List<AlluxioURI> result = mFileSystemMaster.fsck(new AlluxioURI("/"));
    Collections.sort(result);
    Assert.assertEquals(expected, result);
  }

  /**
   * Tests the {@link FileSystemMaster#fsck(AlluxioURI)} method when some files are consistent.
   */
  @Test
  public void fsckPartiallyInconsistent() throws Exception {
    String ufsFile = mFileSystem.getStatus(FILE).getUfsPath();
    UnderFileSystem ufs = UnderFileSystem.get(ufsFile);
    ufs.delete(ufsFile, true);
    List<AlluxioURI> expected = new ArrayList<>();
    expected.add(FILE);
    Assert.assertEquals(expected, mFileSystemMaster.fsck(new AlluxioURI("/")));
  }
}
