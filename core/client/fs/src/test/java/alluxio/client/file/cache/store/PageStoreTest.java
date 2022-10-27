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

import alluxio.ProjectConstants;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.util.io.BufferUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class PageStoreTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {PageStoreType.ROCKS},
        {PageStoreType.LOCAL},
        {PageStoreType.MEM}
    });
  }

  @Parameterized.Parameter
  public PageStoreType mPageStoreType;

  private PageStoreOptions mOptions;

  private PageStore mPageStore;

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    mOptions = new PageStoreOptions();
    mOptions.setStoreType(mPageStoreType);
    mOptions.setPageSize(1024);
    mOptions.setCacheSize(65536);
    mOptions.setAlluxioVersion(ProjectConstants.VERSION);
    mOptions.setRootDir(Paths.get(mTemp.getRoot().getAbsolutePath()));
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
    assertEquals(msgBytes.length, mPageStore.get(id, new ByteArrayTargetBuffer(buf, 0)));
    assertArrayEquals(msgBytes, Arrays.copyOfRange(buf, 0, msgBytes.length));
    mPageStore.delete(id);
    try {
      mPageStore.get(id, new ByteArrayTargetBuffer(buf, 0));
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
      int bytesRead = mPageStore.get(id, offset, len, new ByteArrayTargetBuffer(buf, 0), false);
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
        mPageStore.get(id, offset, len, new ByteArrayTargetBuffer(buf, 0)));
  }

  @Test
  public void getSmallLen() throws Exception {
    int len = 32;
    PageId id = new PageId("0", 0);
    mPageStore.put(id, BufferUtils.getIncreasingByteArray(len));
    byte[] buf = new byte[1024];
    for (int b = 1; b < len; b++) {
      int bytesRead = mPageStore.get(id, 0, b, new ByteArrayTargetBuffer(buf, 0));
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
      int bytesRead = mPageStore.get(id, 0, len, new ByteArrayTargetBuffer(buf, 0));
      assertEquals(b, bytesRead);
      assertArrayEquals(BufferUtils.getIncreasingByteArray(b),
          Arrays.copyOfRange(buf, 0, bytesRead));
    }
  }
}
