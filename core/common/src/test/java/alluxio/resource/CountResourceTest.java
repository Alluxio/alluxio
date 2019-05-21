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

package alluxio.resource;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link CountResource}.
 */
public final class CountResourceTest {
  @Test
  public void flat() {
    AtomicLong count = new AtomicLong(0);
    try (CountResource r = new CountResource(count, true)) {
      // EMPTY
    }
    assertEquals(1, count.get());

    try (CountResource r = new CountResource(count, true)) {
      // EMPTY
    }
    assertEquals(2, count.get());

    try (CountResource r = new CountResource(count, false)) {
      // EMPTY
    }
    assertEquals(1, count.get());
  }

  @Test
  public void nested() {
    AtomicLong count = new AtomicLong(0);
    try (CountResource r1 = new CountResource(count, false)) {
      try (CountResource r2 = new CountResource(count, true)) {
        // EMPTY
      }
      assertEquals(1, count.get());
    }
    assertEquals(0, count.get());
  }
}
