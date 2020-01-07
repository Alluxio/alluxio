/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.client.file.cache.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.client.file.cache.PageStore;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
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

  @Test
  public void test() throws Exception {
    mOptions.setRootDir(mTemp.getRoot().getAbsolutePath());
    try (PageStore store = PageStore.create(mOptions)) {
      helloWorldTest(store);
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
    store.put(0, 0, fromString(msg));
    ByteArrayOutputStream bos = new ByteArrayOutputStream(Constants.KB);
    store.get(0, 0, toChannel(bos));
    String read = new String(bos.toByteArray());
    assertEquals(msg, read);
    store.delete(0, 0);
    try {
      store.get(0, 0, toChannel(bos));
      fail();
    } catch (IOException e) {
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
      store.put(0, pind, Channels.newChannel(new ByteArrayInputStream(b)));
      pages.add(pind);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream(Constants.MB);
    ArrayList<Long> times = new ArrayList<>();
    for (int i = 0; i < numTrials; i++) {
      Collections.shuffle(pages);
      long start = System.nanoTime();
      bos.reset();
      for (Integer pageIndex  : pages) {
        store.get(0, pageIndex, Channels.newChannel(bos));
      }
      long end = System.nanoTime();
      times.add(end - start);
    }
    double avg = (double) times.stream().mapToLong(Long::longValue).sum() / numTrials;
    System.out.println(String.format("Finished thousand get for %7s : %.2fns", mOptions, avg));
  }

  static ReadableByteChannel fromString(String msg) {
    return Channels.newChannel(new ByteArrayInputStream(msg.getBytes()));
  }

  static WritableByteChannel toChannel(ByteArrayOutputStream bos) {
    return Channels.newChannel(bos);
  }
}
