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
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
  private static final int PAGE_SIZE_BYTES = 1024;
  private static final int CACHE_SIZE_BYTES = 65536;
  private static final PageId PAGE_ID1 = new PageId("0L", 0L);
  private static final PageId PAGE_ID2 = new PageId("1L", 1L);
  private static final byte[] PAGE1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);
  private static final byte[] PAGE2 = BufferUtils.getIncreasingByteArray(255, PAGE_SIZE_BYTES);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {new RocksPageStoreOptions()},
        {new LocalPageStoreOptions()},
        {new MemoryPageStoreOptions()},
    });
  }

  @Parameterized.Parameter
  public PageStoreOptions mOptions;

  private PageStore mPageStore;

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mOptions.setPageSize(PAGE_SIZE_BYTES);
    mOptions.setCacheSize(CACHE_SIZE_BYTES);
    mOptions.setAlluxioVersion(ProjectConstants.VERSION);
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
    mPageStore = PageStore.create(mOptions, true);
  }

  @After
  public void after() throws Exception {
    mPageStore.close();
  }

  @Test
  public void getNonExistance() throws Exception {
    byte[] buf = new byte[PAGE1.length];
    mThrown.expect(PageNotFoundException.class);
    mPageStore.get(PAGE_ID1, 0, PAGE1.length, buf, 0);
  }

  @Test
  public void getAfterPut() throws Exception {
    mPageStore.put(PAGE_ID1, PAGE1);
    byte[] buf = new byte[PAGE1.length];
    assertEquals(PAGE1.length, mPageStore.get(PAGE_ID1, 0, PAGE1.length, buf, 0));
    assertArrayEquals(PAGE1, buf);
  }

  @Test
  public void getAfterPutTwice() throws Exception {
    mPageStore.put(PAGE_ID1, PAGE1);
    mPageStore.put(PAGE_ID1, PAGE2);
    byte[] buf = new byte[PAGE2.length];
    assertEquals(PAGE2.length, mPageStore.get(PAGE_ID1, 0, PAGE2.length, buf, 0));
    assertArrayEquals(PAGE2, buf);
  }

  @Test
  public void putSmallPages() throws Exception {
    int[] sizeArray = {1, PAGE_SIZE_BYTES / 2, PAGE_SIZE_BYTES - 1};
    for (int i = 0; i < sizeArray.length; i++) {
      int pageSize = sizeArray[i];
      PageId id = new PageId(String.valueOf(i), 0);
      byte[] page = BufferUtils.getIncreasingByteArray(i, pageSize);
      mPageStore.put(id, page);
    }
    for (int i = 0; i < sizeArray.length; i++) {
      int pageSize = sizeArray[i];
      PageId id = new PageId(String.valueOf(i), 0);
      byte[] buf = new byte[pageSize];
      assertEquals(pageSize, mPageStore.get(id, 0, pageSize, buf, 0));
      assertArrayEquals(BufferUtils.getIncreasingByteArray(i, pageSize), buf);
    }
  }

  @Test
  public void putPagesUntilFull() throws Exception {
    for (int i = 0; i < CACHE_SIZE_BYTES / PAGE_SIZE_BYTES; i++) {
      PageId id = new PageId(String.valueOf(i), 0);
      byte[] page = BufferUtils.getIncreasingByteArray(i, PAGE_SIZE_BYTES - i);
      mPageStore.put(id, page);
    }
    for (int i = 0; i < CACHE_SIZE_BYTES / PAGE_SIZE_BYTES; i++) {
      PageId id = new PageId(String.valueOf(i), 0);
      byte[] buf = new byte[PAGE_SIZE_BYTES - i];
      assertEquals(PAGE_SIZE_BYTES - i, mPageStore.get(id, 0, PAGE_SIZE_BYTES - i, buf, 0));
      assertArrayEquals(BufferUtils.getIncreasingByteArray(i, PAGE_SIZE_BYTES - i), buf);
    }
  }

  @Test
  public void delete() throws Exception {
    mPageStore.put(PAGE_ID1, PAGE1);
    mPageStore.delete(PAGE_ID1);
    mThrown.expect(PageNotFoundException.class);
    byte[] buf = new byte[PAGE1.length];
    mPageStore.get(PAGE_ID1, 0, PAGE1.length, buf, 0);
  }

  @Test
  public void putDeleteRepeated() throws Exception {
    // page store can reclaim resource after deletion
    for (int run = 0; run < 3; run++) {
      // able to add max number of pages
      for (int i = 0; i < CACHE_SIZE_BYTES / PAGE_SIZE_BYTES; i++) {
        PageId id = new PageId(String.valueOf(i), 0);
        mPageStore.put(id, PAGE1);
      }
      // able to remove all added pages
      for (int i = 0; i < CACHE_SIZE_BYTES / PAGE_SIZE_BYTES; i++) {
        PageId id = new PageId(String.valueOf(i), 0);
        mPageStore.delete(id);
      }
    }
  }

  @Test
  public void helloWorldTest() throws Exception {
    String msg = "Hello, World!";
    byte[] msgBytes = msg.getBytes();
    PageId id = new PageId("0", 0);
    mPageStore.put(id, msgBytes);
    byte[] buf = new byte[1024];
    int len = mPageStore.get(id, 0, msgBytes.length, buf, 0);
    String read = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(buf, 0, len)).toString();
    assertEquals(msg, read);
    mPageStore.delete(id);
    try {
      mPageStore.get(id, 0, msgBytes.length, buf, 0);
      fail();
    } catch (PageNotFoundException e) {
      // Test completed successfully;
    }
  }

  @Test
  public void getOffset() throws Exception {
    int len = 32;
    int offset = 3;
    PageId id = new PageId("0", 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    byte[] buf = new byte[len - offset];
    assertEquals(len - offset, mPageStore.get(id, offset, len - offset, buf, 0));
    assertArrayEquals(BufferUtils.getIncreasingByteArray(offset, len - offset), buf);
  }

  @Test
  public void getOffsetOverflow() throws Exception {
    int len = 32;
    int offset = 36;
    PageId id = new PageId("0", 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    byte[] buf = new byte[1024];
    mThrown.expect(IllegalArgumentException.class);
    mPageStore.get(id, offset, len, buf, offset);
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
    for (int i = 0; i < numTrials; i++) {
      Collections.shuffle(pages);
      long start = System.nanoTime();
      bos.reset();
      ByteBuffer buf = ByteBuffer.allocate(Constants.MB);
      for (Integer pageIndex  : pages) {
        buf.clear();
        store.get(new PageId("0", pageIndex)).read(buf);
      }
      long end = System.nanoTime();
      times.add(end - start);
    }
    double avg = (double) times.stream().mapToLong(Long::longValue).sum() / numTrials;
    System.out.println(String.format("Finished thousand get for %7s : %.2fns", mOptions, avg));
  }
}
