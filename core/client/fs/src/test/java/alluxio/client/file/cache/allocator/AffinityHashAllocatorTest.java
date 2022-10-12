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

package alluxio.client.file.cache.allocator;

import static org.junit.Assert.assertEquals;

import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.LocalPageStoreDir;
import alluxio.client.file.cache.store.LocalPageStoreOptions;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class AffinityHashAllocatorTest {
  private InstancedConfiguration mConf = Configuration.copyGlobal();
  private List<PageStoreDir> mDirs;
  private HashAllocator mAllocator;

  @Before
  public void before() {
    LocalPageStoreOptions pageStoreOptions =
        (LocalPageStoreOptions) PageStoreOptions.create(mConf).get(0);
    PageStore pageStore = PageStore.create(pageStoreOptions);
    FIFOCacheEvictor evictor = new FIFOCacheEvictor(mConf);
    LocalPageStoreDir dir1 =
        new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions.setRootDir(
            Paths.get("/1")), pageStore, evictor);
    LocalPageStoreDir dir2 =
        new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions.setRootDir(
            Paths.get("/2")), pageStore, evictor);
    LocalPageStoreDir dir3 =
        new LocalPageStoreDir((LocalPageStoreOptions) pageStoreOptions.setRootDir(
            Paths.get("/3")), pageStore, evictor);
    mDirs = ImmutableList.of(dir1, dir2, dir3);
  }

  @Test
  public void affinityTest() {
    AtomicInteger hashValue = new AtomicInteger(1);
    mAllocator = new AffinityHashAllocator(mDirs, (fileId) -> hashValue.get());
    String fileId = "222";
    //no dir contains the file, it will follow the hash value
    assertEquals("/2", mAllocator.allocate(fileId, 0).getRootPath().toString());
    hashValue.set(2);
    assertEquals("/3", mAllocator.allocate(fileId, 0).getRootPath().toString());
    //put the file to the second dir
    mDirs.get(1).putTempFile(fileId);
    //it will stick to the second dir
    assertEquals("/2", mAllocator.allocate(fileId, 0).getRootPath().toString());
    hashValue.set(0);
    assertEquals("/2", mAllocator.allocate(fileId, 0).getRootPath().toString());
  }
}
