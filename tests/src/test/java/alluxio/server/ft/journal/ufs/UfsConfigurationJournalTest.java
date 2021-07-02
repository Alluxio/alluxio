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

package alluxio.server.ft.journal.ufs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.grpc.MountPOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.MountTable;
import alluxio.resource.CloseableResource;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Map;

/**
 * Integration test to check ufs configuration is persisted after a restart.
 */
public class UfsConfigurationJournalTest {
  private static final String LOCAL_UFS_PATH = Files.createTempDir().getAbsolutePath();

  private FileSystem mFs;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Before
  public void before() throws Exception {
    mFs = mLocalAlluxioClusterResource.get().getClient();
  }

  @Test
  public void testOptionsPersisted() throws Exception {
    // Set ufs specific options and other mount flags
    AlluxioURI mountPoint = new AlluxioURI("/mnt");
    ImmutableMap<String, String> options = ImmutableMap.of("k1", "v1", "k2", "v2");
    mFs.mount(mountPoint, new AlluxioURI(LOCAL_UFS_PATH),
        MountPOptions.newBuilder()
            .putAllProperties(options)
            .setReadOnly(true)
            .setShared(true)
            .build());

    // Get mount id
    MountTable mountTable =
        Whitebox.getInternalState(mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
            .getMasterProcess().getMaster(FileSystemMaster.class), "mMountTable");
    long mountId = mountTable.resolve(mountPoint).getMountId();

    // Restart masters
    mLocalAlluxioClusterResource.get().restartMasters();

    // Checks all options and flags are persisted after restart
    UfsManager ufsManager =
        Whitebox.getInternalState(mLocalAlluxioClusterResource.get().getLocalAlluxioMaster()
            .getMasterProcess().getMaster(FileSystemMaster.class), "mUfsManager");
    try (CloseableResource<UnderFileSystem> resource =
        ufsManager.get(mountId).acquireUfsResource()) {
      UnderFileSystemConfiguration ufsConf = Whitebox.getInternalState(resource.get(), "mConf");
      assertEquals(ufsConf.getMountSpecificConf().size(), options.size());
      for (Map.Entry<String, String> entry : options.entrySet()) {
        assertEquals(entry.getValue(), ufsConf.getMountSpecificConf().get(entry.getKey()));
      }
      assertTrue(ufsConf.isReadOnly());
      assertTrue(ufsConf.isShared());
    }
  }
}
