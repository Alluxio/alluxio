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
import static org.junit.Assert.assertTrue;

import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.LocalPageStoreDir;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashAllocatorTest {
  private InstancedConfiguration mConf = Configuration.copyGlobal();
  private List<PageStoreDir> mDirs;
  private HashAllocator mAllocator;

  @Before
  public void before() {
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    PageStore pageStore = PageStore.create(pageStoreOptions);
    FIFOCacheEvictor evictor = new FIFOCacheEvictor(mConf);
    LocalPageStoreDir dir1 =
        new LocalPageStoreDir(pageStoreOptions.setRootDir(Paths.get("/1")), pageStore, evictor);
    LocalPageStoreDir dir2 =
        new LocalPageStoreDir(pageStoreOptions.setRootDir(Paths.get("/2")), pageStore, evictor);
    LocalPageStoreDir dir3 =
        new LocalPageStoreDir(pageStoreOptions.setRootDir(Paths.get("/3")), pageStore, evictor);
    mDirs = ImmutableList.of(dir1, dir2, dir3);
  }

  @Test
  public void basicHashTest() {
    mAllocator = new HashAllocator(mDirs, Integer::parseInt);
    assertEquals("/1", mAllocator.allocate("0", 0).getRootPath().toString());
    assertEquals("/2", mAllocator.allocate("1", 0).getRootPath().toString());
    assertEquals("/3", mAllocator.allocate("2", 0).getRootPath().toString());
    assertEquals("/1", mAllocator.allocate("3", 0).getRootPath().toString());
  }

  @Test
  public void hashDistributionTest() {
    mAllocator = new HashAllocator(mDirs);
    Map<Path, Integer> result = new HashMap<>();
    int numFiles = 1_000_0;
    for (int i = 0; i < numFiles; i++) {
      PageStoreDir dir = mAllocator.allocate(String.valueOf(i), 0);
      result.put(dir.getRootPath(), result.getOrDefault(dir.getRootPath(), 0) + 1);
    }
    assertTrue(result.values().stream()
        .allMatch(count -> count >= numFiles / mDirs.size() * 0.8
            && count <= numFiles / mDirs.size() * 1.2));
  }
}
