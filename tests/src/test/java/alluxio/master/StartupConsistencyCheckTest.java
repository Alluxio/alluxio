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

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests the consistency check which happens on master start up.
 */
public class StartupConsistencyCheckTest {
  private static final AlluxioURI TOP_LEVEL_FILE = new AlluxioURI("/file");
  private static final AlluxioURI TOP_LEVEL_DIR = new AlluxioURI("/dir");
  private static final AlluxioURI SECOND_LEVEL_FILE = new AlluxioURI("/dir/file");
  private static final AlluxioURI SECOND_LEVEL_DIR = new AlluxioURI("/dir/dir");
  private static final AlluxioURI THIRD_LEVEL_FILE = new AlluxioURI("/dir/dir/file");

  private LocalAlluxioCluster mCluster;
  private FileSystem mFileSystem;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false")
          .setProperty(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED, "true")
          .build();

  @Before
  public void before() throws Exception {
    CreateFileOptions fileOptions = CreateFileOptions.defaults().setWriteType(WriteType.THROUGH);
    CreateDirectoryOptions dirOptions =
        CreateDirectoryOptions.defaults().setWriteType(WriteType.THROUGH);

    mCluster = mLocalAlluxioClusterResource.get();
    mFileSystem = mCluster.getClient();
    mFileSystem.createFile(TOP_LEVEL_FILE, fileOptions).close();
    mFileSystem.createDirectory(TOP_LEVEL_DIR, dirOptions);
    mFileSystem.createDirectory(SECOND_LEVEL_DIR, dirOptions);
    mFileSystem.createFile(SECOND_LEVEL_FILE, fileOptions).close();
    mFileSystem.createFile(THIRD_LEVEL_FILE, fileOptions).close();
  }

  /**
   * Tests that a consistent Alluxio system's startup check does not detect any inconsistencies
   * and completes within 1 minute.
   */
  @Test
  public void consistent() throws Exception {
    mCluster.stopFS();
    MasterRegistry registry = MasterTestUtils.createLeaderFileSystemMasterFromJournal();
    FileSystemMaster master = registry.get(FileSystemMaster.class);
    MasterTestUtils.waitForStartupConsistencyCheck(master);
    Assert.assertTrue(master.getStartupConsistencyCheck().getInconsistentUris().isEmpty());
    registry.stop();
  }

  /**
   * Tests that an inconsistent Alluxio system's startup check correctly detects the inconsistent
   * files.
   */
  @Test
  public void inconsistent() throws Exception {
    String topLevelFileUfsPath = mFileSystem.getStatus(TOP_LEVEL_FILE).getUfsPath();
    String secondLevelDirUfsPath = mFileSystem.getStatus(SECOND_LEVEL_DIR).getUfsPath();
    mCluster.stopFS();
    UnderFileSystem ufs = UnderFileSystem.Factory.get(topLevelFileUfsPath);
    ufs.deleteFile(topLevelFileUfsPath);
    ufs.deleteDirectory(secondLevelDirUfsPath, DeleteOptions.defaults().setRecursive(true));
    MasterRegistry registry = MasterTestUtils.createLeaderFileSystemMasterFromJournal();
    FileSystemMaster master = registry.get(FileSystemMaster.class);
    MasterTestUtils.waitForStartupConsistencyCheck(master);
    List<AlluxioURI> expected =
        Lists.newArrayList(TOP_LEVEL_FILE, SECOND_LEVEL_DIR, THIRD_LEVEL_FILE);
    List<AlluxioURI> result = master.getStartupConsistencyCheck().getInconsistentUris();
    Collections.sort(expected);
    Collections.sort(result);
    Assert.assertEquals(expected, result);
    registry.stop();
  }
}
