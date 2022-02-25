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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.function.Function;

@RunWith(Parameterized.class)
public class LocalPageStoreTest {

  @Parameterized.Parameter
  public int mRootDirCount;

  public List<TemporaryFolder> mTempList;

  private static final byte[] TEST_PAGE = "test".getBytes();
  private static final int TEST_PAGE_SIZE = TEST_PAGE.length;
  private static final FileInfo TEST_FILE_INFO = new FileInfo(CacheScope.GLOBAL, 100);

  @Rule
  public TemporaryFolder mTemp1 = new TemporaryFolder();
  @Rule
  public TemporaryFolder mTemp2 = new TemporaryFolder();

  private LocalPageStoreOptions mOptions;

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][] {{1}, {2}});
  }

  @Before
  public void before() {
    mOptions = new LocalPageStoreOptions();
    mTempList = new ArrayList<>();
    if (mRootDirCount == 1) {
      mTempList.add(mTemp1);
    } else if (mRootDirCount == 2) {
      mTempList.add(mTemp1);
      mTempList.add(mTemp2);
    }
    List<Path> rootDir = mTempList.stream().map(
        temp ->
            Paths.get(temp.getRoot().getAbsolutePath())).collect(Collectors.toList());
    mOptions.setRootDirs(rootDir);
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
    long actualCount = 0;
    for (Path root : mOptions.getRootDirs()) {
      actualCount += Files.list(
          Paths.get(root.toString(), Long.toString(mOptions.getPageSize()))).count();
    }
    assertEquals(mRootDirCount, actualCount);
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
    long actualCount = 0;
    for (Path root : mOptions.getRootDirs()) {
      actualCount += Files.list(
          Paths.get(root.toString(), Long.toString(mOptions.getPageSize()))).count();
    }
    assertEquals(10, actualCount);
  }

  @Test
  public void cleanFileAndDirectory() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    PageId pageId = new PageId("0", 0);
    PageInfo pageInfo = new PageInfo(pageId, TEST_PAGE_SIZE, TEST_FILE_INFO);
    pageStore.put(pageInfo, TEST_PAGE);
    Path p = pageStore.getPageFilePath(pageInfo);
    assertTrue(Files.exists(p));
    pageStore.delete(pageInfo);
    assertFalse(Files.exists(p));
    assertFalse(Files.exists(p.getParent()));
  }

  @Test
  public void reloadAllPagesFromDisk() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    for (int i = 0; i < 5; i++) {
      for (int pageIndex = 0; pageIndex < 5; pageIndex++) {
        PageId id = new PageId(Integer.toString(i), pageIndex);
        FileInfo fileInfo = new FileInfo(CacheScope.create("test.table.p" + i), 100);
        pageStore.put(new PageInfo(id, TEST_PAGE_SIZE, fileInfo), TEST_PAGE);
      }
    }
    Map<PageId, PageInfo> pages = pageStore.getPages()
        .collect(Collectors.toMap(PageInfo::getPageId, Function
            .identity()));
    assertEquals(25, pages.size());
    for (int i = 0; i < 5; i++) {
      for (int pageIndex = 0; pageIndex < 5; pageIndex++) {
        PageId id = new PageId(Integer.toString(i), pageIndex);
        assertEquals("test.table.p" + i,
            pages.get(id).getFileInfo().getScope().getScopeId());
        assertEquals(100,
            pages.get(id).getFileInfo().getLastModificationTimeMs());
      }
    }
  }

  @Test
  public void reloadAllPagesWithMixedModifiedTime() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    String testFileId = "testfile";
    //put stale pages, lastModificationTime is 100
    for (int pageIndex = 0; pageIndex < 5; pageIndex++) {
      PageId id = new PageId(testFileId, pageIndex);
      FileInfo fileInfo = new FileInfo(CacheScope.GLOBAL, 100);
      pageStore.put(new PageInfo(id, TEST_PAGE_SIZE, fileInfo), TEST_PAGE);
    }
    //put new pages, lastModificationTime is 200
    for (int pageIndex = 0; pageIndex < 5; pageIndex++) {
      PageId id = new PageId(testFileId, pageIndex);
      FileInfo fileInfo = new FileInfo(CacheScope.GLOBAL, 200);
      pageStore.put(new PageInfo(id, TEST_PAGE_SIZE, fileInfo), TEST_PAGE);
    }
    Map<PageId, PageInfo> pages = pageStore.getPages()
        .collect(Collectors.toMap(PageInfo::getPageId, Function
            .identity()));
    assertEquals(5, pages.size());
    for (int pageIndex = 0; pageIndex < 5; pageIndex++) {
      PageId id = new PageId(testFileId, pageIndex);
      assertEquals(CacheScope.GLOBAL,
          pages.get(id).getFileInfo().getScope());
      assertEquals(200,
          pages.get(id).getFileInfo().getLastModificationTimeMs());
    }
  }

  @Test
  public void cleanStalePageFiles() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    String testFileId = "testfile";
    PageId id = new PageId(testFileId, 0);
    //put a stale page
    byte[] stalePage = "stale page".getBytes();
    FileInfo staleFileInfo = new FileInfo(CacheScope.GLOBAL, 100);
    PageInfo stalePageInfo = new PageInfo(id, stalePage.length, staleFileInfo);
    pageStore.put(stalePageInfo, stalePage);
    //put a new page with the same page id
    byte[] newPage = "new page".getBytes();
    FileInfo newFileInfo = new FileInfo(CacheScope.GLOBAL, 200);
    PageInfo newPageInfo = new PageInfo(id, newPage.length, newFileInfo);
    pageStore.put(newPageInfo, newPage);

    byte[] buf = new byte[1024];
    assertEquals(newPage.length, pageStore.get(newPageInfo, buf));
    assertArrayEquals(newPage, Arrays.copyOfRange(buf, 0, newPage.length));

    assertFalse("check if page file path has been deleted",
        Files.exists(pageStore.getPageFilePath(stalePageInfo)));
    assertFalse("check if modification timestamp path has been deleted",
        Files.exists(pageStore.getPageFilePath(stalePageInfo).getParent()));
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
