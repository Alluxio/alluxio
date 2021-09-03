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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.client.file.cache.FileInfo;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.quota.CacheScope;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class LocalPageStoreTest {

  private static final byte[] TEST_PAGE = "test".getBytes();
  private static final int TEST_PAGE_SIZE = TEST_PAGE.length;
  private static final FileInfo TEST_FILE_INFO = new FileInfo(CacheScope.GLOBAL, 100);

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  private LocalPageStoreOptions mOptions;

  @Before
  public void before() {
    mOptions = new LocalPageStoreOptions();
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
  }

  @Test
  public void testPutGetDefault() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    helloWorldTest(pageStore);
  }

  @Test
  public void testSingleFileBucket() throws Exception {
    mOptions.setFileBuckets(1);
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    long numFiles = 100;
    for (int i = 0; i < numFiles; i++) {
      PageId id = new PageId(Integer.toString(i), 0);
      pageStore.put(new PageInfo(id, TEST_PAGE_SIZE, TEST_FILE_INFO), TEST_PAGE);
    }
    assertEquals(1, Files.list(
        Paths.get(mOptions.getRootDir(), Long.toString(mOptions.getPageSize()))).count());
  }

  @Test
  public void testMultiFileBucket() throws Exception {
    int numBuckets = 10;
    mOptions.setFileBuckets(numBuckets);
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    long numFiles = numBuckets * 10;
    for (int i = 0; i < numFiles; i++) {
      PageId id = new PageId(Integer.toString(i), 0);
      pageStore.put(new PageInfo(id, TEST_PAGE_SIZE, TEST_FILE_INFO), TEST_PAGE);
    }
    assertEquals(10, Files.list(
        Paths.get(mOptions.getRootDir(), Long.toString(mOptions.getPageSize()))).count());
  }

  @Test
  public void cleanFileAndDirectory() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    PageId pageId = new PageId("0", 0);
    PageInfo pageInfo = new PageInfo(pageId, TEST_PAGE_SIZE, TEST_FILE_INFO);
    pageStore.put(pageInfo, TEST_PAGE);
    Path p = pageStore.getFilePath(pageInfo);
    assertTrue(Files.exists(p));
    pageStore.delete(pageInfo);
    assertFalse(Files.exists(p));
    assertFalse(Files.exists(p.getParent()));
  }

  private void helloWorldTest(PageStore store) throws Exception {
    String msg = "Hello, World!";
    PageId id = new PageId("0", 0);
    PageInfo pageInfo = new PageInfo(id, msg.getBytes().length, TEST_FILE_INFO);
    store.put(pageInfo, msg.getBytes());
    byte[] buf = new byte[1024];
    assertEquals(msg.getBytes().length,
        store.get(pageInfo, buf));
    assertArrayEquals(msg.getBytes(), Arrays.copyOfRange(buf, 0, msg.getBytes().length));
  }
}
