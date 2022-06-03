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

package alluxio.client.file.cache.store;

import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationTestUtils;
import alluxio.ProjectConstants;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PageStoreDirTest {
  private InstancedConfiguration mConf = ConfigurationTestUtils.copyDefaults();

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {new RocksPageStoreOptions()},
        {new LocalPageStoreOptions()}
    });
  }

  @Parameterized.Parameter
  public PageStoreOptions mOptions;
  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  private PageStoreDir mPageStoreDir;

  @Before
  public void before() throws Exception {
    mOptions.setPageSize(1024);
    mOptions.setCacheSize(65536);
    mOptions.setAlluxioVersion(ProjectConstants.VERSION);
    mOptions.setRootDir(Paths.get(mTemp.getRoot().getAbsolutePath()));
    mPageStoreDir = PageStoreDir.createPageStoreDir(mConf, mOptions);
  }

  @After
  public void after() throws Exception {
    mPageStoreDir.getPageStore().close();
  }

  @Test
  public void getPages() throws Exception {
    int len = 32;
    int count = 16;
    byte[] data = BufferUtils.getIncreasingByteArray(len);
    Set<PageInfo> pages = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      PageId id = new PageId("0", i);
      mPageStoreDir.getPageStore().put(id, data);
      pages.add(new PageInfo(id, data.length, mPageStoreDir));
    }
    Set<PageInfo> restored = new HashSet<>();
    mPageStoreDir.restorePages((pageInfo -> restored.add(pageInfo)));
    assertEquals(pages, restored);
  }

  @Test
  public void getPagesUUID() throws Exception {
    int len = 32;
    int count = 16;
    byte[] data = BufferUtils.getIncreasingByteArray(len);
    Set<PageInfo> pages = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      PageId id = new PageId(UUID.randomUUID().toString(), i);
      mPageStoreDir.getPageStore().put(id, data);
      pages.add(new PageInfo(id, data.length, mPageStoreDir));
    }
    Set<PageInfo> restored = new HashSet<>();
    mPageStoreDir.restorePages((pageInfo -> restored.add(pageInfo)));
    assertEquals(pages, restored);
  }
}
