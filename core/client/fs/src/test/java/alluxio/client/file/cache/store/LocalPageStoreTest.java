package alluxio.client.file.cache.store;

import static org.junit.Assert.assertEquals;

import alluxio.client.file.cache.PageStore;

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

  @Test
  public void testPutGetDefault() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mTemp.getRoot().getAbsolutePath());
    helloWorldTest(pageStore);
  }

  @Test
  public void testSmallBuffer() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mTemp.getRoot().getAbsolutePath(), 1, 1);
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
