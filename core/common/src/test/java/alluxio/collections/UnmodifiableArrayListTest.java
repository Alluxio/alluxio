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
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Arrays;

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

    array[3] = null;
    assertEquals(Arrays.asList(array), list);
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
    assertFalse(list.contains("e"));

    // Update on the underlying array will be reflected
    array[0] = "e";
    for (int i = 0; i < array.length; i++) {
      String s = array[i];
      assertTrue(list.contains(s));
      assertEquals(i, list.indexOf(s));
      assertEquals(i, list.lastIndexOf(s));
    }
    assertFalse(list.contains("a"));
  }
}
