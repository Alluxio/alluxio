package alluxio.client.file.cache.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Before;
import org.junit.Test;

import alluxio.test.util.ConcurrencyUtils;

public class ScopeEncoderTest {
  private static final int BITS_PER_SCOPE = 8; // 256 scopes at most
  private static final int NUM_SCOPES = (1 << BITS_PER_SCOPE);
  private static final ScopeInfo SCOPE1 = new ScopeInfo("table1");

  // concurrency configurations
  private static final int DEFAULT_THREAD_AMOUNT = 12;
  private static final int DEFAULT_TIMEOUT_SECONDS = 10;

  private ScopeEncoder scopeEncoder;

  @Before
  public void init() {
    scopeEncoder = new ScopeEncoder(BITS_PER_SCOPE);
  }

  @Test
  public void testBasic() {
    int id = scopeEncoder.encode(SCOPE1);
    assertEquals(0, id);
    assertEquals(SCOPE1, scopeEncoder.decode(id));
  }

  @Test
  public void testConcurrentEncodeDecode() throws Exception {
    List<Runnable> runnables = new ArrayList<>();
    for (int k = 0; k < DEFAULT_THREAD_AMOUNT; k++) {
      runnables.add(() -> {
        for (int i = 0; i < NUM_SCOPES * 16; i++) {
          int r = ThreadLocalRandom.current().nextInt(NUM_SCOPES);
          ScopeInfo scopeInfo = new ScopeInfo("table" + r);
          int id = scopeEncoder.encode(scopeInfo);
          assertEquals(scopeInfo, scopeEncoder.decode(id));
          assertEquals(id, scopeEncoder.encode(scopeInfo));
          assertTrue(0 <= id && id < NUM_SCOPES);
        }
      });
    }
    ConcurrencyUtils.assertConcurrent(runnables, DEFAULT_TIMEOUT_SECONDS);
  }
}
