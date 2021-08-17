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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class PageStoreTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {new RocksPageStoreOptions()},
        {new LocalPageStoreOptions()}
    });
  }

  @Parameterized.Parameter
  public PageStoreOptions mOptions;

  private PageStore mPageStore;

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mOptions.setPageSize(1024);
    mOptions.setCacheSize(65536);
    mOptions.setAlluxioVersion(ProjectConstants.VERSION);
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
    mPageStore = PageStore.create(mOptions);
  }

  @After
  public void after() throws Exception {
    mPageStore.close();
  }

  @Test
  public void helloWorldTest() throws Exception {
    String msg = "Hello, World!";
    byte[] msgBytes = msg.getBytes();
    PageId id = new PageId("0", 0);
    mPageStore.put(id, msgBytes);
    byte[] buf = new byte[1024];
    assertEquals(msgBytes.length, mPageStore.get(id, buf));
    assertArrayEquals(msgBytes, Arrays.copyOfRange(buf, 0, msgBytes.length));
    mPageStore.delete(id);
    try {
      mPageStore.get(id, buf);
      fail();
    } catch (PageNotFoundException e) {
      // Test completed successfully;
    }
  }

  @Test
  public void getOffset() throws Exception {
    int len = 32;
    PageId id = new PageId("0", 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    byte[] buf = new byte[len];
    for (int offset = 1; offset < len; offset++) {
      int bytesRead = mPageStore.get(id, offset, len, buf, 0);
      assertEquals(len - offset, bytesRead);
      assertArrayEquals(BufferUtils.getIncreasingByteArray(offset, len - offset),
          Arrays.copyOfRange(buf, 0, bytesRead));
    }
  }

  @Test
  public void getOffsetOverflow() throws Exception {
    int len = 32;
    int offset = 36;
    PageId id = new PageId("0", 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    byte[] buf = new byte[1024];
    assertThrows(IllegalArgumentException.class, () ->
        mPageStore.get(id, offset, len, buf, 0));
  }

  @Test
  public void getPages() throws Exception {
    int len = 32;
    int count = 16;
    byte[] data = BufferUtils.getIncreasingByteArray(len);
    Set<PageInfo> pages = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      PageId id = new PageId("0", i);
      mPageStore.put(id, data);
      pages.add(new PageInfo(id, data.length));
    }
    Set<PageInfo> restored = mPageStore.getPages().collect(Collectors.toSet());
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
      mPageStore.put(id, data);
      pages.add(new PageInfo(id, data.length));
    }
    Set<PageInfo> restored = mPageStore.getPages().collect(Collectors.toSet());
    assertEquals(pages, restored);
  }

  @Test
  public void getSmallLen() throws Exception {
    int len = 32;
    PageId id = new PageId("0", 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    byte[] buf = new byte[1024];
    for (int b = 1; b < len; b++) {
      int bytesRead = mPageStore.get(id, 0, b, buf, 0);
      assertEquals(b, bytesRead);
      assertArrayEquals(BufferUtils.getIncreasingByteArray(b),
          Arrays.copyOfRange(buf, 0, bytesRead));
    }
  }

  @Test
  public void getSmallBuffer() throws Exception {
    int len = 32;
    PageId id = new PageId("0", 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    for (int b = 1; b < len; b++) {
      byte[] buf = new byte[b];
      int bytesRead = mPageStore.get(id, 0, len, buf, 0);
      assertEquals(b, bytesRead);
      assertArrayEquals(BufferUtils.getIncreasingByteArray(b),
          Arrays.copyOfRange(buf, 0, bytesRead));
    }
  }

  @Ignore
  @Test
  public void perfTest() throws Exception {
    thousandGetTest(mPageStore);
  }

  void thousandGetTest(PageStore store) throws Exception {
    int numPages = 1000;
    int numTrials = 3;
    // Fill the cache
    List<Integer> pages = new ArrayList<>(numPages);
    byte[] b = new byte[Constants.MB];
    Arrays.fill(b, (byte) 0x7a);
    Random r = new Random();
    for (int i = 0; i < numPages; i++) {
      int pind = r.nextInt();
      store.put(new PageId("0", pind), b);
      pages.add(pind);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream(Constants.MB);
    ArrayList<Long> times = new ArrayList<>();
    byte[] buf = new byte[Constants.MB];
    for (int i = 0; i < numTrials; i++) {
      Collections.shuffle(pages);
      long start = System.nanoTime();
      bos.reset();
      for (Integer pageIndex : pages) {
        store.get(new PageId("0", pageIndex), buf);
      }
      long end = System.nanoTime();
      times.add(end - start);
    }
    double avg = (double) times.stream().mapToLong(Long::longValue).sum() / numTrials;
    System.out.println(String.format("Finished thousand get for %7s : %.2fns", mOptions, avg));
  }
}
