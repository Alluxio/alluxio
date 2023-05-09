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

package alluxio.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Ticker;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RateLimiterTest {

  private final Ticker mTicker = new Ticker() {
    @Override
    public long read() {
      return mTime;
    }
  };

  private long mTime;

  @Before
  public void before() {
    mTime = 0;
  }

  @Test
  public void testFastRequests() {
    long permitsPerSecond = 10;
    long timePerPermit = Duration.ofSeconds(1).toNanos() / permitsPerSecond;
    SimpleRateLimiter rateLimiter = new SimpleRateLimiter(permitsPerSecond, mTicker);

    // if the timer is moving as fast as the permits then there should be no waiting
    for (int i = 0; i < 10; i++) {
      mTime += timePerPermit;
      assertFalse(rateLimiter.acquire().isPresent());
    }
    // if we move forward a large amount, we should still only get 1 new permit
    mTime += timePerPermit * 100;
    assertFalse(rateLimiter.acquire().isPresent());
    assertTrue(rateLimiter.acquire().isPresent());

    mTime += timePerPermit;
    assertTrue(rateLimiter.acquire().isPresent());

    mTime += timePerPermit * 2;
    assertFalse(rateLimiter.acquire().isPresent());

    Optional<Long> permit = rateLimiter.acquire();
    assertTrue(permit.isPresent());
    mTime += timePerPermit;
    assertEquals(mTime, (long) permit.get());
  }

  @Test
  public void testSlowRequests() {
    long permitsPerSecond = 10;
    long timePerPermit = Duration.ofSeconds(1).toNanos() / permitsPerSecond;
    SimpleRateLimiter rateLimiter = new SimpleRateLimiter(permitsPerSecond, mTicker);
    List<Long> permits = new ArrayList<>();
    for (int i = 0; i < permitsPerSecond; i++) {
      Optional<Long> permit = rateLimiter.acquire();
      assertTrue(permit.isPresent());
      permits.add(permit.get());
    }
    assertEquals(Duration.ofSeconds(1).toNanos(), (long) permits.get(permits.size() - 1));
    for (int i = 0; i < permitsPerSecond; i++) {
      mTime += timePerPermit;
      assertEquals(0, rateLimiter.getWaitTimeNanos(permits.get(i)));
    }
  }
}
