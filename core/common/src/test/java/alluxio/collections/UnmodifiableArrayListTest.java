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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test for {@link UnmodifiableArrayList}.
 */
public class UnmodifiableArrayListTest {
  @Test
  public void view() {
    String[] array = new String[]{"a", "b", "c", "d"};
    UnmodifiableArrayList<String> list = new UnmodifiableArrayList<>(array);
    assertEquals(Arrays.asList(array), list);

    // Update on the underlying array will be reflected
    array[0] = "e";
    assertEquals(Arrays.asList(array), list);
    assertEquals(array.length, list.size());
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], list.get(i));
    }

    array[3] = null;
    assertEquals(Arrays.asList(array), list);
    assertEquals(array.length, list.size());
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], list.get(i));
    }
  }

  @Test
  public void iterate() {
    String[] array = new String[]{"a", "b", "c", "d"};
    UnmodifiableArrayList<String> list = new UnmodifiableArrayList<>(array);

    assertEquals(ImmutableList.copyOf(Arrays.asList(array).iterator()),
        ImmutableList.copyOf(list.iterator()));

    // Update on the underlying array will be reflected
    array[0] = "e";
    assertEquals(ImmutableList.copyOf(Arrays.asList(array).iterator()),
        ImmutableList.copyOf(list.iterator()));
    AtomicInteger i = new AtomicInteger(0);
    list.iterator().forEachRemaining((element) -> {
      assertEquals(array[i.get()], element);
      i.getAndIncrement();
    });
  }

  @Test
  public void lookup() {
    String[] array = new String[]{"a", "b", "c", "d"};
    UnmodifiableArrayList<String> list = new UnmodifiableArrayList<>(array);

    for (int i = 0; i < array.length; i++) {
      String s = array[i];
      assertTrue(list.contains(s));
      assertEquals(i, list.indexOf(s));
      assertEquals(i, list.lastIndexOf(s));
    }
    assertEquals(-1, list.indexOf("e"));
    assertEquals(-1, list.lastIndexOf("e"));
    assertTrue(list.containsAll(Arrays.asList(array)));
    assertFalse(list.containsAll(ImmutableList.of("a", "b", "x")));
    assertFalse(list.contains("e"));

    // Update on the underlying array will be reflected
    array[0] = "e";
    for (int i = 0; i < array.length; i++) {
      String s = array[i];
      assertTrue(list.contains(s));
      assertEquals(i, list.indexOf(s));
      assertEquals(i, list.lastIndexOf(s));
    }
    assertEquals(-1, list.indexOf("a"));
    assertEquals(-1, list.lastIndexOf("a"));
    assertTrue(list.containsAll(Arrays.asList(array)));
    assertFalse(list.containsAll(ImmutableList.of("a", "b", "x")));
    assertFalse(list.contains("a"));
  }

  @Test
  public void noModification() {
    String[] array = new String[]{"a", "b", "c", "d"};
    UnmodifiableArrayList<String> list = new UnmodifiableArrayList<>(array);

    assertThrows(UnsupportedOperationException.class, () -> {
      list.set(0, "e");
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.add("e");
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.add(0, "x");
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.addAll(ImmutableList.of("x", "y"));
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.addAll(0, ImmutableList.of("x", "y"));
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.remove("a");
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.remove(0);
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.removeAll(ImmutableList.of("a", "b"));
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.iterator().remove();
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.listIterator().add("x");
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.listIterator().remove();
    });
    assertThrows(UnsupportedOperationException.class, () -> {
      list.listIterator().set("a");
    });
  }

  @Test
  public void empty() {
    String[] array = new String[]{};
    UnmodifiableArrayList<String> list = new UnmodifiableArrayList<>(array);
    assertEquals(0, list.size());
    assertTrue(list.isEmpty());
  }

  @Test
  public void nullRejected() {
    String[] array = null;
    assertThrows(NullPointerException.class, () -> {
      UnmodifiableArrayList<String> list = new UnmodifiableArrayList<>(array);
    });
  }

  @Test
  public void toArray() {
    String[] array = new String[]{"a", "b", "c", "d"};
    UnmodifiableArrayList<String> list = new UnmodifiableArrayList<>(array);
    assertArrayEquals(array, list.toArray());

    array[0] = "c";
    assertArrayEquals(array, list.toArray());
  }
}
