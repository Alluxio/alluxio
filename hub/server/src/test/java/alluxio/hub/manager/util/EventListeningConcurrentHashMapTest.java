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

package alluxio.hub.manager.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class EventListeningConcurrentHashMapTest {

  private ExecutorService mSvc = MoreExecutors.newDirectExecutorService();

  @Test
  public void addEvent() {
    EventListeningConcurrentHashMap<Integer, Boolean> map =
        new EventListeningConcurrentHashMap<>(mSvc);
    AtomicInteger i = new AtomicInteger(0);
    Runnable e = i::getAndIncrement;
    map.registerEventListener(e, 0);
    map.put(5, true);
    assertEquals(1, i.get());
    map.put(6, false);
    assertEquals(2, i.get());
    map.put(7, true);
    assertEquals(3, i.get());
    map.put(6, false); // should not trigger an event
    assertEquals(3, i.get());
    map.putIfAbsent(5, false);
    assertEquals(3, i.get());
    assertEquals(map.get(5), true);
  }

  @Test
  public void removeEvent() {
    EventListeningConcurrentHashMap<Integer, Boolean> map =
        new EventListeningConcurrentHashMap<>(mSvc);
    AtomicInteger i = new AtomicInteger(0);
    Runnable e = i::getAndIncrement;
    map.registerEventListener(e, 0);
    map.put(5, true);
    assertEquals(1, i.get());
    map.put(6, false);
    assertEquals(2, i.get());
    map.put(7, true);
    assertEquals(3, i.get());
    map.remove(5);
    assertEquals(4, i.get());
    map.remove(6);
    assertEquals(5, i.get());
    map.remove(7);
    assertEquals(6, i.get());
    map.remove(5);
    assertEquals(6, i.get());
  }

  @Test
  public void testEventTiming() throws Exception {
    EventListeningConcurrentHashMap<Integer, Boolean> map =
        new EventListeningConcurrentHashMap<>(mSvc);
    AtomicInteger i = new AtomicInteger(0);
    Runnable e = i::getAndIncrement;
    long timeout = 250L;
    map.registerEventListener(e, timeout);
    long start = System.currentTimeMillis();
    map.put(5, true);
    map.put(6, false);
    long total = System.currentTimeMillis() - start;
    // In case the machine is *very* slow
    if (total < timeout) {
      assertEquals(1, i.get());
    } else {
      assertEquals(2, i.get());
    }
    int current = i.get();
    Thread.sleep(timeout);
    map.put(5, true); // should not trigger event
    assertEquals(current, i.get());
    map.put(7, true);
    assertEquals(current + 1, i.get());
  }

  @Test
  public void testMultipleListeners() {
    EventListeningConcurrentHashMap<Integer, Boolean> map =
        new EventListeningConcurrentHashMap<>(mSvc);
    AtomicInteger i = new AtomicInteger(0);
    map.registerEventListener(i::getAndIncrement, 0);
    map.registerEventListener(i::incrementAndGet, 0);
    assertEquals(0, i.get());
    map.put(5, true);
    assertEquals(2, i.get());
    map.put(6, true);
    assertEquals(4, i.get());
    map.put(6, true);
    assertEquals(4, i.get());
  }

  @Test
  public void testEquals() {
    EventListeningConcurrentHashMap<Integer, Boolean> map1 =
        new EventListeningConcurrentHashMap<>(mSvc);
    EventListeningConcurrentHashMap<Integer, Boolean> map2 =
        new EventListeningConcurrentHashMap<>(mSvc);
    assertFalse(map1.equals(null));
    assertFalse(map1.equals(new Object()));
    assertTrue(map1.equals(map2));
    map1.put(5, true);
    map2.put(6, false);
    assertNotEquals(map1, map2);
    map2.put(5, true);
    map1.put(6, false);
    assertEquals(map1, map2);
  }

  @Test
  public void testHashCode() {
    EventListeningConcurrentHashMap<Integer, Boolean> map1 =
        new EventListeningConcurrentHashMap<>(mSvc);
    EventListeningConcurrentHashMap<Integer, Boolean> map2 =
        new EventListeningConcurrentHashMap<>(mSvc);
    assertEquals(map1.hashCode(), map2.hashCode());
    map1.put(5, true);
    map2.put(6, false);
    assertNotEquals(map1.hashCode(), map2.hashCode());
    map2.put(5, true);
    map1.put(6, false);
    assertEquals(map1.hashCode(), map2.hashCode());
  }

  @Test
  public void testEventListenerEquals() {
    Runnable run = System::currentTimeMillis;
    EventListeningConcurrentHashMap.EventListener e =
        new EventListeningConcurrentHashMap.EventListener(run, 100);
    EventListeningConcurrentHashMap.EventListener e2 =
        new EventListeningConcurrentHashMap.EventListener(run, 200);
    EventListeningConcurrentHashMap.EventListener e3 =
        new EventListeningConcurrentHashMap.EventListener(run, 100);
    EventListeningConcurrentHashMap.EventListener e4 =
        new EventListeningConcurrentHashMap.EventListener(System::currentTimeMillis, 100);
    assertFalse(e.equals(null));
    assertFalse(e.equals(new Object()));
    assertFalse(e.equals(e2));
    assertFalse(e.equals(e4));
    assertTrue(e.equals(e3));
  }
}
