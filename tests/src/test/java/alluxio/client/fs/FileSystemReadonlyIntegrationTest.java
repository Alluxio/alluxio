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

package alluxio.client.fs;

import static junit.framework.TestCase.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.master.file.FileSystemMaster;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.wire.MountPointInfo;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

/**
 * Test behavior of {@link FileSystemMaster}, when the paths are readonly.
 */
public class FileSystemReadonlyIntegrationTest {
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
          new LocalAlluxioClusterResource.Builder()
                  .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY, true)
                  .build();
  private FileSystem mFileSystem;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    FileSystemContext fsCtx = FileSystemContext.create(ServerConfiguration.global());
    fsCtx.getClientContext().loadConf(fsCtx.getMasterAddress(), true, true);
    mFileSystem = sLocalAlluxioClusterResource.get().getClient(fsCtx);
  }

  @Test
  public void rootMountIsReadonly() throws Exception {
    Map<String, MountPointInfo> mountTable = mFileSystem.getMountTable();
    MountPointInfo rootInfo = mountTable.get("/");
    assertTrue(rootInfo.getReadOnly());
  }

  @Test
  public void rootMountWriteDenied() throws Exception {
    // Write operation under a readonly path is not permitted
    mThrown.expect(AccessControlException.class);
    mFileSystem.createFile(new AlluxioURI("/test"));
  }
}
