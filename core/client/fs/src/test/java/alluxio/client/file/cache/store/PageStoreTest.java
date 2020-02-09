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

import static alluxio.client.file.cache.store.RocksPageStore.KEY_LEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.client.file.cache.PageId;
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
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

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

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

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
    PageId id = new PageId(0, 0);
    mPageStore.put(id, msgBytes);
    assertEquals(1, mPageStore.pages());
    ByteBuffer buf = ByteBuffer.allocate(1024);
    mPageStore.get(id).read(buf);
    buf.flip();
    String read = StandardCharsets.UTF_8.decode(buf).toString();
    assertEquals(msg, read);
    mPageStore.delete(id, msgBytes.length);
    assertEquals(0, mPageStore.pages());
    try {
      buf.clear();
      mPageStore.get(id).read(buf);
      fail();
    } catch (PageNotFoundException e) {
      // Test completed successfully;
    }
  }

  @Test
  public void getOffset() throws Exception {
    int len = 32;
    int offset = 3;
    PageId id = new PageId(0, 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    ByteBuffer buf = ByteBuffer.allocate(1024);
    try (ReadableByteChannel channel = mPageStore.get(id, offset)) {
      channel.read(buf);
    }
    buf.flip();
    assertTrue(BufferUtils.equalIncreasingByteBuffer(offset, len - offset, buf));
  }

  @Test
  public void getOffsetOverflow() throws Exception {
    int len = 32;
    int offset = 36;
    PageId id = new PageId(0, 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    ByteBuffer buf = ByteBuffer.allocate(1024);
    mThrown.expect(IllegalArgumentException.class);
    try (ReadableByteChannel channel = mPageStore.get(id, offset)) {
      channel.read(buf);
    }
  }

  @Test
  public void getPages() throws Exception {
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
    int len = 32;
    int count = 16;
    byte[] data = BufferUtils.getIncreasingByteArray(len);
    Set<PageId> pages = new HashSet<>(count);
    try (PageStore store = PageStore.create(mOptions)) {
      for (int i = 0; i < count; i++) {
        PageId id = new PageId(0, i);
        store.put(id, data);
        pages.add(id);
      }
    }
    try (PageStore store = PageStore.create(mOptions)) {
      assertEquals(pages, new HashSet<>(store.getPages()));
    }
  }

  @Ignore
  @Test
  public void testPagesAndBytes() throws Exception {
    long totalBytes = 0;
    for (int i = 1; i <= 1024; i++) {
      PageId id = new PageId(i, 0);
      mPageStore.put(id, new byte[i]);
      if (mOptions.getType() == PageStoreType.LOCAL) {
        totalBytes += i;
      } else {
        totalBytes += KEY_LEN + i;
      }
      assertEquals(i, mPageStore.pages());
      assertEquals(totalBytes, mPageStore.bytes());
    }
    for (int i = 1024; i >= 1; i--) {
      PageId id = new PageId(i, 0);
      mPageStore.delete(id, i);
      if (mOptions.getType() == PageStoreType.LOCAL) {
        totalBytes -= i;
      } else {
        totalBytes -= KEY_LEN + i;
      }
      assertEquals(i - 1, mPageStore.pages());
      assertEquals(totalBytes, mPageStore.bytes());
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
      store.put(new PageId(0, pind), b);
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
        store.get(new PageId(0, pageIndex)).read(buf);
      }
      long end = System.nanoTime();
      times.add(end - start);
    }
    double avg = (double) times.stream().mapToLong(Long::longValue).sum() / numTrials;
    System.out.println(String.format("Finished thousand get for %7s : %.2fns", mOptions, avg));
  }
}
