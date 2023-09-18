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

package alluxio.worker.dora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

public class DoraMetaManagerTest {
  DoraMetaManager mManager;
  DoraUfsManager doraUfsManager;

  @Before
  public void before() throws IOException {
    AlluxioProperties prop = new AlluxioProperties();
    prop.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR, "~/alluxio/metasotre");
    AlluxioConfiguration conf = new InstancedConfiguration(prop);
    PagedDoraWorker worker = mock(PagedDoraWorker.class);
    CacheManager cacheManager = mock(CacheManager.class);
    doraUfsManager = mock(DoraUfsManager.class);
    mManager = new DoraMetaManager(conf, worker, cacheManager, doraUfsManager);
  }

  @After
  public void after() {
    try {
      mManager.close();
    } catch (IOException e) {
      mManager = null;
    }
  }

  @Test
  public void testListFromUfsListUfsWhenFail() throws IOException {
    UnderFileSystem system = mock(UnderFileSystem.class);
    doThrow(new IOException()).when(system).listStatus(anyString(), any());
    doReturn(system).when(doraUfsManager).getOrAdd(any(), any());

    assertThrows(IOException.class, () -> {
      mManager.listFromUfs("/test", false);
    });
  }

  @Test
  public void testListFromUfsGetWhenNull() throws IOException {
    UnderFileSystem system = mock(UnderFileSystem.class);
    when(system.listStatus(anyString())).thenReturn(null);
    when(system.getStatus(anyString())).thenReturn(null);
    doReturn(system).when(doraUfsManager).getOrAdd(any(), any());

    Optional<UfsStatus[]> status = mManager.listFromUfs("/test", false);
    assertEquals(status, Optional.empty());
  }

  @Test
  public void testListFromUfsGetWhenFail() throws IOException {
    UnderFileSystem system = mock(UnderFileSystem.class);
    when(system.listStatus(anyString())).thenReturn(null);
    when(system.getStatus(anyString())).thenThrow(new FileNotFoundException());
    doReturn(system).when(doraUfsManager).getOrAdd(any(), any());

    Optional<UfsStatus[]> status = mManager.listFromUfs("/test", false);
    assertEquals(status, Optional.empty());
  }

  @Test
  public void testListFromUfsGetWhenSuccess() throws IOException {
    UnderFileSystem system = mock(UnderFileSystem.class);
    UfsStatus fakeStatus = mock(UfsStatus.class);
    when(system.listStatus(anyString())).thenReturn(null);
    when(system.getStatus(anyString())).thenReturn(fakeStatus);
    doReturn(system).when(doraUfsManager).getOrAdd(any(), any());

    when(fakeStatus.getName()).thenReturn("test");

    Optional<UfsStatus[]> status = mManager.listFromUfs("/test", false);
    assertEquals(status.get()[0].getName(), "test");
  }
}
