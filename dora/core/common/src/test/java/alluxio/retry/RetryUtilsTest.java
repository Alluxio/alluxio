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

package alluxio.retry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link RetryUtils}.
 */
public class RetryUtilsTest {
  @Test
  public void success() throws IOException {
    AtomicInteger count = new AtomicInteger(0);
    RetryUtils.retry("success test", () -> {
      count.incrementAndGet();
      if (count.get() == 5) {
        return;
      }
      throw new IOException("Fail");
    }, new CountingRetry(10));
    assertEquals(5, count.get());
  }

  @Test
  public void failure() throws IOException {
    AtomicInteger count = new AtomicInteger(0);
    try {
      RetryUtils.retry("failure test", () -> {
        count.incrementAndGet();
        throw new IOException(Integer.toString(count.get()));
      }, new CountingRetry(10));
      fail("Expected an exception to be thrown");
    } catch (IOException e) {
      assertEquals("11", e.getMessage());
    }
    assertEquals(11, count.get());
  }
}
