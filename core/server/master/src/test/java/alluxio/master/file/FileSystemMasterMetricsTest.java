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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.file.DefaultFileSystemMaster.Metrics;
import alluxio.metrics.MasterMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.worker.UfsClientCache;
import alluxio.worker.UfsClientCache.UfsClient;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultFileSystemMaster.Metrics}.
 */
public class FileSystemMasterMetricsTest {
  private FileSystemMaster mFileSystemMaster;
  private UfsClientCache mUfsClientCache;

  @Before
  public void before() throws Exception {
    MetricsSystem.clearAllMetrics();
    mFileSystemMaster = Mockito.mock(FileSystemMaster.class);
    mUfsClientCache = Mockito.mock(UfsClientCache.class);
    Metrics.registerGauges(mFileSystemMaster, mUfsClientCache);
  }

  @Test
  public void testMetricsFilesPinned() {
    when(mFileSystemMaster.getNumberOfPinnedFiles()).thenReturn(100);
    assertEquals(100, getGauge(MasterMetrics.FILES_PINNED));
  }

  @Test
  public void testMetricsPathsTotal() {
    when(mFileSystemMaster.getNumberOfPaths()).thenReturn(90);
    assertEquals(90, getGauge(MasterMetrics.PATHS_TOTAL));
  }

  @Test
  public void testMetricsUfsCapacity() throws Exception {
    UfsClient client = Mockito.mock(UfsClient.class);
    UnderFileSystem ufs = Mockito.mock(UnderFileSystem.class);
    String ufsDataFolder = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    when(ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_TOTAL)).thenReturn(1000L);
    when(ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_USED)).thenReturn(200L);
    when(ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_FREE)).thenReturn(800L);
    when(client.acquireUfsResource()).thenReturn(new CloseableResource<UnderFileSystem>(ufs) {
      @Override
      public void close() {}
    });
    when(mUfsClientCache.getRoot()).thenReturn(client);
    assertEquals(1000L, getGauge(MasterMetrics.UFS_CAPACITY_TOTAL));
    assertEquals(200L, getGauge(MasterMetrics.UFS_CAPACITY_USED));
    assertEquals(800L, getGauge(MasterMetrics.UFS_CAPACITY_FREE));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricsSystem.getMetricName(name))
        .getValue();
  }
}
