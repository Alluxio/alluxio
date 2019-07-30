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

package alluxio.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ConcurrentIdentityHashMapTest {


  private ConcurrentIdentityHashMap<String, String> mMap;

  @Before
  public void before() {
    mMap = new ConcurrentIdentityHashMap<>();
  }

  @Test
  public void testIdentitySemantics() {
    ConcurrentIdentityHashMap<String, Boolean> m = new ConcurrentIdentityHashMap<>();
    String k = new String("test");
    String k2 = new String("test");
    m.put(k, true);
    m.put(k2, false);
    assertFalse(m.isEmpty());
    assertEquals(2, m.size());
    assertTrue(m.containsKey(k));
    assertTrue(m.containsKey(k2));
    assertEquals(2, m.entrySet().size());
    for(Map.Entry e : m.entrySet()) {
      assertTrue(e.getKey() == k || e.getKey() == k2);
    }
    m.remove("test"); // Should not remove, because it doesn't have the correct obj ref
    assertEquals(2, m.size());
    m.remove(k); // remove with correct identity ref
    assertEquals(1, m.size());
    m.remove(k); // Remove twice should not work
    m.remove(k2);
    assertEquals(0, m.size()); // remove with correct identity ref
  }

  @Test
  public void putIfAbsent() {
    String t = new String("test");
    mMap.putIfAbsent(t, "v1");
    assertEquals(1, mMap.size());
    assertEquals("v1", mMap.get(t));
    assertFalse(mMap.containsKey("test"));
  }

  @Test
  public void keySet() {
    String x = new String("x");
    String xx = new String("x");
    mMap.put(x, "x");
    mMap.put(xx, "x2");
    assertEquals(2, mMap.size());
    Set<String> km = mMap.keySet();
    assertEquals(2, km.size());
    assertTrue(km.contains(x));
    assertTrue(km.contains(xx));
    mMap.remove(x);
    assertEquals(1, km.size());
    assertTrue(km.remove(xx));
    assertEquals(0, km.size());
    assertEquals(0, mMap.size());
  }

  @Test
  public void replace() {
    String x = new String("x");
    String x2 = new String("x");
    mMap.put(x, "x");
    mMap.replace(x2, "x");
    assertEquals(1, mMap.size());
    assertEquals("x", mMap.replace(x, "y"));
    mMap.replace(x, "noreplace", "z"); // shouldn't replace
    assertEquals("y", mMap.get(x));
  }

  @Test
  public void remove() {
    String x = new String("x");
    String x2 = new String("x");
    mMap.put(x, "y");
    assertEquals(1, mMap.size());
    assertNull(mMap.remove(x2));
    assertEquals(1, mMap.size());
    assertEquals("y", mMap.remove(x));
    assertEquals(0, mMap.size());

    mMap.put(x2, "z");
    assertEquals(1, mMap.size());
    assertFalse(mMap.remove(x2, "a"));
    assertTrue(mMap.remove(x2, "z"));
    assertEquals(0, mMap.size());
  }

  @Test
  public void values() {
    String x1 = new String("x");
    String x2 = new String("x");
    mMap.put(x1, "z");
    mMap.put(x2, "z");
    Collection<String> v = mMap.values();
    assertEquals(2, v.size());
    v.forEach(val -> assertEquals("z", val));
    mMap.remove(x1);
    assertEquals(1, v.size());
  }

  @Test
  public void clear() {
    String x1 = new String("x");
    String x2 = new String("x");
    mMap.put(x1, "z");
    mMap.put(x2, "z");
    assertEquals(2, mMap.size());
    mMap.clear();
    assertEquals(0, mMap.size());
    assertEquals(0, mMap.keySet().size());
    assertEquals(0, mMap.entrySet().size());
  }
}
