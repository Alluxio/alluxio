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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.client.file.cache.PageId;
import alluxio.exception.PageNotFoundException;
import alluxio.client.file.cache.PageStore;
import alluxio.util.io.BufferUtils;

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
import java.util.List;
import java.util.Random;

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

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Test
  public void test() throws Exception {
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
    try (PageStore store = PageStore.create(mOptions)) {
      helloWorldTest(store);
    }
  }

  @Test
  public void getOffset() throws Exception {
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
    int len = 32;
    int offset = 3;
    try (PageStore store = PageStore.create(mOptions)) {
      PageId id = new PageId(0, 0);
      store.put(id, BufferUtils.getIncreasingByteArray(len));
      ByteBuffer buf = ByteBuffer.allocate(1024);
      try (ReadableByteChannel channel = store.get(id, offset)) {
        channel.read(buf);
      }
      buf.flip();
      assertTrue(BufferUtils.equalIncreasingByteBuffer(offset, len - offset, buf));
    }
  }

  @Test
  public void getOffsetOverflow() throws Exception {
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
    int len = 32;
    int offset = 36;
    try (PageStore store = PageStore.create(mOptions)) {
      PageId id = new PageId(0, 0);
      store.put(id, BufferUtils.getIncreasingByteArray(len));
      ByteBuffer buf = ByteBuffer.allocate(1024);
      mThrown.expect(IllegalArgumentException.class);
      try (ReadableByteChannel channel = store.get(id, offset)) {
        channel.read(buf);
      }
    }
  }

  @Ignore
  @Test
  public void perfTest() throws Exception {
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
    try (PageStore store = PageStore.create(mOptions)) {
      thousandGetTest(store);
    }
  }

  void helloWorldTest(PageStore store) throws Exception {
    String msg = "Hello, World!";
    PageId id = new PageId(0, 0);
    store.put(id, msg.getBytes());
    ByteBuffer buf = ByteBuffer.allocate(1024);
    store.get(id).read(buf);
    buf.flip();
    String read = StandardCharsets.UTF_8.decode(buf).toString();
    assertEquals(msg, read);
    store.delete(id);
    try {
      buf.clear();
      store.get(id).read(buf);
      fail();
    } catch (PageNotFoundException e) {
      // Test completed successfully;
    }
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
