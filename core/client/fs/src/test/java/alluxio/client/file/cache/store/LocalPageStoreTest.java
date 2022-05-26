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

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;

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

@RunWith(Parameterized.class)
public class LocalPageStoreTest {

  @Parameterized.Parameter
  public int mRootDirCount;

  public List<TemporaryFolder> mTempList;

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
      pageStore.put(id, "test".getBytes());
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
      pageStore.put(id, "test".getBytes());
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
    pageStore.put(pageId, "test".getBytes());
    Path p = pageStore.getFilePath(pageId);
    assertTrue(Files.exists(p));
    pageStore.delete(pageId);
    assertFalse(Files.exists(p));
    assertFalse(Files.exists(p.getParent()));
  }

  private void helloWorldTest(PageStore store) throws Exception {
    String msg = "Hello, World!";
    PageId id = new PageId("0", 0);
    store.put(id, msg.getBytes());
    byte[] buf = new byte[1024];
    assertEquals(msg.getBytes().length, store.get(id, buf));
    assertArrayEquals(msg.getBytes(), Arrays.copyOfRange(buf, 0, msg.getBytes().length));
  }
}
