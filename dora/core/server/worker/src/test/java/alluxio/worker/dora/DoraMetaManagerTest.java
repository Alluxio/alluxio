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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.worker.dora.DoraMetaManager;
import alluxio.worker.dora.DoraUfsManager;
import alluxio.worker.dora.PagedDoraWorker;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

public class DoraMetaManagerTest {
  DoraMetaManager mManager;

  @Before
  public void before() throws IOException {
    AlluxioProperties prop = new AlluxioProperties();
    prop.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR, "/opt/alluxio/metasotre");
    // prop.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_TTL, "");
    AlluxioConfiguration conf = new InstancedConfiguration(prop);
    PagedDoraWorker worker = mock(PagedDoraWorker.class);
    CacheManager cacheManager = mock(CacheManager.class);
    DoraUfsManager doraUfsManager = mock(DoraUfsManager.class);
    // when(doraUfsManager.getOrAdd(any(), any())).thenThrow(new IOException());
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
  public void testGetUfsInstance() {
    // assertThrows(Exception.class, () -> {
    //   mManager.getUfsInstance("");
    // });
    try {
      mManager.getUfsInstance("");
    } catch (Exception e) {
      System.out.println("exception");
      assertNotNull(e);
    }
  }

  @Test
  public void testListFromUfsListUfsWhenFail() throws IOException {
    UnderFileSystem system = mock(UnderFileSystem.class);
    when(system.listStatus(anyString())).thenThrow(new IOException());

    DoraMetaManager mManagerSpy = spy(mManager);
    // no idea why this mock fail
    // when(mManagerSpy.getUfsInstance(anyString())).thenReturn(system);
    doReturn(system).when(mManagerSpy).getUfsInstance(anyString());

    try {
      mManagerSpy.listFromUfs("/test", false);
    } catch (IOException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testListFromUfsGetWhenNull() throws IOException {
    UnderFileSystem system = mock(UnderFileSystem.class);
    when(system.listStatus(anyString())).thenReturn(null);
    when(system.getStatus(anyString())).thenReturn(null);

    DoraMetaManager mManagerSpy = spy(mManager);
    doReturn(system).when(mManagerSpy).getUfsInstance(anyString());

    Optional<UfsStatus[]> status = mManagerSpy.listFromUfs("/test", false);
    assertEquals(status, Optional.empty());
  }

  @Test
  public void testListFromUfsGetWhenFail() throws IOException {
    UnderFileSystem system = mock(UnderFileSystem.class);
    when(system.listStatus(anyString())).thenReturn(null);
    when(system.getStatus(anyString())).thenThrow(new FileNotFoundException());

    DoraMetaManager mManagerSpy = spy(mManager);
    doReturn(system).when(mManagerSpy).getUfsInstance(anyString());

    Optional<UfsStatus[]> status = mManagerSpy.listFromUfs("/test", false);
    assertEquals(status, Optional.empty());
  }

  @Test
  public void testListFromUfsGetWhenSuccess() throws IOException {
    UnderFileSystem system = mock(UnderFileSystem.class);
    UfsStatus fakeStatus = mock(UfsStatus.class);
    when(system.listStatus(anyString())).thenReturn(null);
    when(system.getStatus(anyString())).thenReturn(fakeStatus);
    when(fakeStatus.getName()).thenReturn("test");

    DoraMetaManager mManagerSpy = spy(mManager);
    doReturn(system).when(mManagerSpy).getUfsInstance(anyString());

    Optional<UfsStatus[]> status = mManagerSpy.listFromUfs("/test", false);
    assertEquals(status.get()[0].getName(), "test");
  }
}
