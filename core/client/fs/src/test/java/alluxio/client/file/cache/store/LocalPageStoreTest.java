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

import alluxio.client.file.cache.PageStore;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class LocalPageStoreTest {

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
  public void testSmallBuffer() throws Exception {
    mOptions.setBufferSize(1)
        .setBufferPoolSize(1);
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    helloWorldTest(pageStore);
  }

  void helloWorldTest(PageStore store) throws Exception {
    String msg = "Hello, World!";
    store.put(0, 0, fromString(msg));
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    store.get(0, 0, toChannel(bos));
    String read = new String(bos.toByteArray());
    assertEquals(msg, read);
  }

  static ReadableByteChannel fromString(String msg) {
    return Channels.newChannel(new ByteArrayInputStream(msg.getBytes()));
  }

  static WritableByteChannel toChannel(ByteArrayOutputStream bos) {
    return Channels.newChannel(bos);
  }
}
